import grpc
from concurrent import futures
import logging
import os
import sys

# Импорты сгенерированных файлов
import catalog_pb2
import catalog_pb2_grpc
import file_pb2
import file_pb2_grpc
import auth_pb2
import auth_pb2_grpc

from pymongo import MongoClient
from bson.objectid import ObjectId

# --- Настройки ---
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("CatalogService")

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://mongo:27017/')
AUTH_SERVICE_ADDR = os.getenv('AUTH_SERVICE_ADDR', 'auth-service:50053')
FILE_SERVICE_ADDR = os.getenv('FILE_SERVICE_ADDR', 'file-service:50052')
LISTEN_PORT = os.getenv('PORT', '50051')

# --- База данных ---
try:
    client = MongoClient(MONGO_URI)
    db = client['catalog_db']
    dirs_col = db['directories']
    files_col = db['files']
    client.server_info()
    logger.info(f"Connected to MongoDB at {MONGO_URI}")
except Exception as e:
    logger.critical(f"Failed to connect to MongoDB: {e}")
    sys.exit(1)


class AuthInterceptor(grpc.ServerInterceptor):
    """
    Пропускает запросы на чтение (Get) для проверки прав внутри сервиса.
    Блокирует запросы на изменение (Create/Update/Delete) для анонимов.
    """
    def __init__(self):
        self.channel = grpc.insecure_channel(AUTH_SERVICE_ADDR)
        self.stub = auth_pb2_grpc.AuthStub(self.channel)

    def intercept_service(self, continuation, handler_call_details):
        method_name = handler_call_details.method
        is_read_op = "Get" in method_name.split('/')[-1]

        metadata = dict(handler_call_details.invocation_metadata)
        token = metadata.get('authorization')
        if token and token.startswith('Bearer '):
            token = token.split(' ')[1]
        
        is_authenticated = False
        if token:
            try:
                self.stub.GetCurrentUser(auth_pb2.SessionData(SessionID=token))
                is_authenticated = True
            except:
                is_authenticated = False

        # Если авторизован - проходим
        if is_authenticated:
            return continuation(handler_call_details)
        else:
            # Если аноним и это чтение - проходим (сервис проверит публичность)
            if is_read_op:
                return continuation(handler_call_details)
            # Если аноним хочет менять данные - отказ
            else:
                return self._abort(grpc.StatusCode.UNAUTHENTICATED, "Auth required")

    def _abort(self, code, details):
        def abort_handler(request, context):
            context.abort(code, details)
        return grpc.unary_unary_rpc_method_handler(abort_handler)


class FileServiceClient:
    def __init__(self):
        self.channel = grpc.insecure_channel(FILE_SERVICE_ADDR)
        self.stub = file_pb2_grpc.FileStub(self.channel)

    def delete_files(self, external_ids):
        if not external_ids: return
        for ext_id in external_ids:
            try:
                self.stub.DeleteFile(file_pb2.DeleteFileRequest(UUID=ext_id, UserID=1))
            except Exception as e:
                logger.error(f"File Service error: {e}")


class CatalogService(catalog_pb2_grpc.CatalogServiceServicer):
    def __init__(self):
        self.file_service = FileServiceClient()
        self.auth_channel = grpc.insecure_channel(AUTH_SERVICE_ADDR)
        self.auth_stub = auth_pb2_grpc.AuthStub(self.auth_channel)

    # --- Вспомогательные методы проверок ---

    def _get_user_identity(self, context):
        """Возвращает (user_id, is_authenticated)"""
        meta = dict(context.invocation_metadata())
        token = meta.get('authorization')
        if not token: return (None, False)
        if token.startswith('Bearer '): token = token.split(' ')[1]
        try:
            user = self.auth_stub.GetCurrentUser(auth_pb2.SessionData(SessionID=token))
            return (user.ID, True)
        except:
            return (None, False)

    def _can_read(self, doc, user_id):
        """
        Главная проверка доступа на чтение.
        Возвращает True, если пользователь имеет право видеть этот объект.
        """
        # 1. Публичный объект видят все
        if doc.get('is_public', False): 
            return True
        
        # 2. Если объект приватный и пользователь аноним - доступ закрыт
        if user_id is None: 
            return False
        
        # 3. Проверка списков доступа
        # Используем .get() для безопасности, если запись старая
        if user_id == doc.get('owner_id'): return True
        if user_id in doc.get('allowed_readers', []): return True
        if user_id in doc.get('allowed_writers', []): return True
        
        return False

    def _can_write(self, doc, user_id):
        """Проверка прав на изменение (Владелец + Редакторы)"""
        if user_id is None: return False
        
        if user_id == doc.get('owner_id'): return True
        if user_id in doc.get('allowed_writers', []): return True
        return False

    # --- Directory RPCs ---

    def CreateDirectory(self, request, context):
        user_id, is_auth = self._get_user_identity(context)
        if not is_auth: context.abort(grpc.StatusCode.UNAUTHENTICATED, "Auth required")

        doc = {
            "name": request.name, 
            "parent_id": request.parent_id if request.parent_id else None,
            "is_public": request.is_public,
            "owner_id": user_id,
            "allowed_readers": list(request.allowed_readers),
            "allowed_writers": list(request.allowed_writers)
        }
        
        try:
            res = dirs_col.insert_one(doc)
            return catalog_pb2.DirectoryResponse(
                id=str(res.inserted_id), name=doc['name'], parent_id=str(doc['parent_id'] or ""),
                is_public=doc['is_public'], owner_id=doc['owner_id'],
                allowed_readers=doc['allowed_readers'], allowed_writers=doc['allowed_writers']
            )
        except Exception as e:
            logger.error(f"DB Error: {e}")
            context.abort(grpc.StatusCode.INTERNAL, "Write failed")

    def UpdateDirectory(self, request, context):
        user_id, is_auth = self._get_user_identity(context)
        if not is_auth: context.abort(grpc.StatusCode.UNAUTHENTICATED, "Auth required")

        doc = dirs_col.find_one({"_id": ObjectId(request.id)})
        if not doc: context.abort(grpc.StatusCode.NOT_FOUND, "Directory not found")

        if not self._can_write(doc, user_id):
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "No write permission")

        update_fields = {}
        if request.name: 
            update_fields["name"] = request.name
        if request.parent_id != "no_change":
            update_fields["parent_id"] = request.parent_id if request.parent_id else None
        
        # Редакторы могут менять настройки доступа
        if request.is_public != doc.get('is_public'):
            update_fields["is_public"] = request.is_public
            
        if request.allowed_readers:
            update_fields["allowed_readers"] = list(request.allowed_readers)
        if request.allowed_writers:
            update_fields["allowed_writers"] = list(request.allowed_writers)

        try:
            if update_fields:
                dirs_col.update_one({"_id": ObjectId(request.id)}, {"$set": update_fields})
            
            updated = dirs_col.find_one({"_id": ObjectId(request.id)})
            return catalog_pb2.DirectoryResponse(
                id=str(updated["_id"]), name=updated["name"], 
                parent_id=str(updated.get("parent_id") or ""), 
                is_public=updated.get('is_public', False),
                owner_id=updated.get('owner_id'),
                allowed_readers=updated.get('allowed_readers', []),
                allowed_writers=updated.get('allowed_writers', [])
            )
        except Exception as e:
            logger.error(f"DB Error: {e}")
            context.abort(grpc.StatusCode.INTERNAL, "Update failed")

    def DeleteDirectory(self, request, context):
        user_id, is_auth = self._get_user_identity(context)
        if not is_auth: context.abort(grpc.StatusCode.UNAUTHENTICATED, "Auth required")

        doc = dirs_col.find_one({"_id": ObjectId(request.id)})
        if not doc: context.abort(grpc.StatusCode.NOT_FOUND, "Not found")

        # Удаление только Владельцем
        if doc.get('owner_id') != user_id:
             context.abort(grpc.StatusCode.PERMISSION_DENIED, "Only owner can delete")

        if dirs_col.count_documents({"parent_id": request.id}) > 0:
             return catalog_pb2.DeleteResponse(success=False, message="Directory not empty")
        
        files_col.delete_many({"parent_directory_id": request.id})
        dirs_col.delete_one({"_id": ObjectId(request.id)})
        return catalog_pb2.DeleteResponse(success=True, message="Deleted")

    def GetDirectoryContent(self, request, context):
        """
        Основной метод получения контента с проверкой прав.
        """
        user_id, _ = self._get_user_identity(context)
        pid = request.directory_id if request.directory_id else None

        # 1. ЗАЩИТА ПАПКИ: Если запрашиваем конкретную папку, проверяем, пустят ли нас внутрь.
        if pid:
            target_dir = dirs_col.find_one({"_id": ObjectId(pid)})
            if not target_dir: 
                context.abort(grpc.StatusCode.NOT_FOUND, "Directory not found")
            
            # Если прав на чтение нет -> ВЫБРАСЫВАЕМ ОШИБКУ
            if not self._can_read(target_dir, user_id):
                logger.warning(f"Access DENIED for user {user_id} to dir {pid}")
                context.abort(grpc.StatusCode.PERMISSION_DENIED, "You do not have permission to view this directory")

        # 2. ФИЛЬТРАЦИЯ КОНТЕНТА: Загружаем всё содержимое...
        all_subdirs = dirs_col.find({"parent_id": pid})
        all_files = files_col.find({"parent_directory_id": pid})

        visible_dirs = []
        visible_files = []

        # ... и оставляем только то, что пользователю можно видеть.
        for d in all_subdirs:
            if self._can_read(d, user_id):
                visible_dirs.append(catalog_pb2.DirectoryResponse(
                    id=str(d["_id"]), name=d["name"], parent_id=str(d.get("parent_id") or ""),
                    is_public=d.get('is_public', False), owner_id=d.get('owner_id'),
                    allowed_readers=d.get('allowed_readers', []), allowed_writers=d.get('allowed_writers', [])
                ))

        for f in all_files:
            if self._can_read(f, user_id):
                visible_files.append(catalog_pb2.FileResponse(
                    id=str(f["_id"]), name=f["name"], external_file_id=f["external_file_id"],
                    is_public=f.get('is_public', False), owner_id=f.get('owner_id'),
                    allowed_readers=f.get('allowed_readers', []), allowed_writers=f.get('allowed_writers', [])
                ))

        return catalog_pb2.DirectoryContentResponse(directories=visible_dirs, files=visible_files)

    # --- File RPCs ---

    def RegisterFile(self, request, context):
        user_id, is_auth = self._get_user_identity(context)
        if not is_auth: context.abort(grpc.StatusCode.UNAUTHENTICATED, "Auth required")
        
        doc = {
            "name": request.name, 
            "parent_directory_id": request.parent_directory_id, 
            "external_file_id": request.external_file_id,
            "is_public": request.is_public,
            "owner_id": user_id,
            "allowed_readers": list(request.allowed_readers),
            "allowed_writers": list(request.allowed_writers)
        }
        res = files_col.insert_one(doc)
        return catalog_pb2.FileResponse(
            id=str(res.inserted_id), name=request.name, external_file_id=request.external_file_id,
            is_public=doc['is_public'], owner_id=doc['owner_id'],
            allowed_readers=doc['allowed_readers'], allowed_writers=doc['allowed_writers']
        )

    def UpdateFile(self, request, context):
        user_id, is_auth = self._get_user_identity(context)
        if not is_auth: context.abort(grpc.StatusCode.UNAUTHENTICATED, "Auth required")

        doc = files_col.find_one({"_id": ObjectId(request.id)})
        if not doc: context.abort(grpc.StatusCode.NOT_FOUND, "File not found")

        if not self._can_write(doc, user_id):
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "No write permission")

        update_fields = {}
        if request.name: 
            update_fields["name"] = request.name
        
        if request.is_public != doc.get('is_public'):
            update_fields["is_public"] = request.is_public
            
        if request.allowed_readers:
             update_fields["allowed_readers"] = list(request.allowed_readers)
        if request.allowed_writers:
             update_fields["allowed_writers"] = list(request.allowed_writers)

        if update_fields:
            files_col.update_one({"_id": ObjectId(request.id)}, {"$set": update_fields})
        
        updated = files_col.find_one({"_id": ObjectId(request.id)})
        return catalog_pb2.FileResponse(
            id=str(updated["_id"]), name=updated["name"], external_file_id=updated["external_file_id"],
            is_public=updated.get('is_public', False), owner_id=updated.get('owner_id'),
            allowed_readers=updated.get('allowed_readers', []), allowed_writers=updated.get('allowed_writers', [])
        )
    
    def DeleteFile(self, request, context):
        user_id, is_auth = self._get_user_identity(context)
        if not is_auth: context.abort(grpc.StatusCode.UNAUTHENTICATED, "Auth required")

        doc = files_col.find_one({"_id": ObjectId(request.id)})
        if not doc: context.abort(grpc.StatusCode.NOT_FOUND, "File not found")
        
        if doc.get('owner_id') != user_id:
             context.abort(grpc.StatusCode.PERMISSION_DENIED, "Only owner can delete file")

        files_col.delete_one({"_id": ObjectId(request.id)})
        self.file_service.delete_files([doc['external_file_id']])
        return catalog_pb2.DeleteResponse(success=True, message="Deleted")

def serve():
    auth_interceptor = AuthInterceptor()
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        interceptors=[auth_interceptor]
    )
    catalog_pb2_grpc.add_CatalogServiceServicer_to_server(CatalogService(), server)
    server.add_insecure_port(f'[::]:{LISTEN_PORT}')
    logger.info(f"Catalog Service started on port {LISTEN_PORT}")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
