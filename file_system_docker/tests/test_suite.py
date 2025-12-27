import unittest
import grpc
import uuid
import time
import concurrent.futures
import random
import logging
from pymongo import MongoClient

# Сгенерированные grpc модули
import catalog_pb2
import catalog_pb2_grpc
import auth_pb2
import auth_pb2_grpc
import file_pb2
import file_pb2_grpc

class TestCatalogComplete(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        print("\n[Setup] Establishing connections...")
        cls.catalog_channel = grpc.insecure_channel('localhost:50051')
        cls.catalog_stub = catalog_pb2_grpc.CatalogServiceStub(cls.catalog_channel)
        
        cls.auth_channel = grpc.insecure_channel('localhost:50053')
        cls.auth_stub = auth_pb2_grpc.AuthStub(cls.auth_channel)

        cls.file_channel = grpc.insecure_channel('localhost:50052')
        cls.file_stub = file_pb2_grpc.FileStub(cls.file_channel)

        #Сброс БД, НЕ ВЫПОЛЯТЬ ТЕСТЫ НА PROD
        try:
            client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000)
            client.drop_database('catalog_db')
            print("[Setup] Database 'catalog_db' dropped.")
            client.close()
        except Exception as e:
            print(f"[Setup] Warning: Could not drop database: {e}")

    @classmethod
    def tearDownClass(cls):
        cls.catalog_channel.close()
        cls.auth_channel.close()
        cls.file_channel.close()

    def create_session(self, user_id, name):
        #запуск сессии
        session_id = f"sess-{name}-{uuid.uuid4().hex[:8]}"
        self.auth_stub.CreateSession(auth_pb2.FullUserData(
            user=auth_pb2.User(ID=user_id, Email=f"{name}@test.com", IsAuth=True),
            SessionID=session_id
        ))
        return (('authorization', f'Bearer {session_id}'),)

    def setUp(self):
        #Запускать перед каждым тестом, создает пользователей в заглушке авторизации
        self.id_owner = 100
        self.meta_owner = self.create_session(self.id_owner, "owner")

        self.id_writer = 200
        self.meta_writer = self.create_session(self.id_writer, "writer")

        self.id_reader = 300
        self.meta_reader = self.create_session(self.id_reader, "reader")

        self.id_stranger = 999
        self.meta_stranger = self.create_session(self.id_stranger, "stranger")
        
        self.metadata = self.meta_owner

    # ==========================================
    # Часть 1: функицональные тесты
    # ==========================================

    #создание корневого каталога
    def test_01_create_root_directory(self):
        dirname = f"Root_{uuid.uuid4().hex[:6]}"
        resp = self.catalog_stub.CreateDirectory(
            catalog_pb2.CreateDirectoryRequest(name=dirname, parent_id=""),
            metadata=self.metadata
        )
        self.assertTrue(resp.id)
        self.assertEqual(resp.name, dirname)
    #создание подкаталога
    def test_02_create_subdirectory(self):
        parent = self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="Parent", parent_id=""), metadata=self.metadata)
        child = self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="Child", parent_id=parent.id), metadata=self.metadata)
        self.assertEqual(child.parent_id, parent.id)
    #получить пустой выход от пустой директории
    def test_03_get_directory_content_empty(self):
        d = self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="EmptyDir", parent_id=""), metadata=self.metadata)
        content = self.catalog_stub.GetDirectoryContent(catalog_pb2.GetDirectoryRequest(directory_id=d.id), metadata=self.metadata)
        self.assertEqual(len(content.files), 0)
    #заполнить директорию
    def test_04_get_directory_content_populated(self):
        root = self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="RootPop", parent_id=""), metadata=self.metadata)
        self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="Sub1", parent_id=root.id), metadata=self.metadata)
        self.catalog_stub.RegisterFile(catalog_pb2.RegisterFileRequest(name="f.txt", parent_directory_id=root.id, external_file_id="x"), metadata=self.metadata)
        
        content = self.catalog_stub.GetDirectoryContent(catalog_pb2.GetDirectoryRequest(directory_id=root.id), metadata=self.metadata)
        self.assertEqual(len(content.directories), 1)
        self.assertEqual(len(content.files), 1)
    #переименовать директорию
    def test_05_rename_directory(self):
        d = self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="OldName", parent_id=""), metadata=self.metadata)
        updated = self.catalog_stub.UpdateDirectory(
            catalog_pb2.UpdateDirectoryRequest(id=d.id, name="NewName", parent_id="no_change"),
            metadata=self.metadata
        )
        self.assertEqual(updated.name, "NewName")
    #переместить директорию
    def test_06_move_directory(self):
        folder_a = self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="Folder A", parent_id=""), metadata=self.metadata)
        folder_b = self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="Folder B", parent_id=""), metadata=self.metadata)
        updated_b = self.catalog_stub.UpdateDirectory(
            catalog_pb2.UpdateDirectoryRequest(id=folder_b.id, parent_id=folder_a.id),
            metadata=self.metadata
        )
        self.assertEqual(updated_b.parent_id, folder_a.id)
    #арегистрировать файл
    def test_07_register_file(self):
        d = self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="Docs", parent_id=""), metadata=self.metadata)
        f = self.catalog_stub.RegisterFile(
            catalog_pb2.RegisterFileRequest(name="resume.pdf", parent_directory_id=d.id, external_file_id="ext-uuid-1"),
            metadata=self.metadata
        )
        self.assertEqual(f.name, "resume.pdf")
    #удалить файл
    def test_08_delete_file(self):
        d = self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="DocsDel", parent_id=""), metadata=self.metadata)
        f = self.catalog_stub.RegisterFile(catalog_pb2.RegisterFileRequest(name="del.txt", parent_directory_id=d.id, external_file_id="ext-uuid-2"), metadata=self.metadata)
        res = self.catalog_stub.DeleteFile(catalog_pb2.DeleteFileRequest(id=f.id), metadata=self.metadata)
        self.assertTrue(res.success)
    #удалить директорию с файлами
    def test_09_delete_directory_with_files(self):
        d = self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="WithFiles", parent_id=""), metadata=self.metadata)
        self.catalog_stub.RegisterFile(catalog_pb2.RegisterFileRequest(name="f1", parent_directory_id=d.id, external_file_id="e1"), metadata=self.metadata)
        res = self.catalog_stub.DeleteDirectory(catalog_pb2.DeleteDirectoryRequest(id=d.id), metadata=self.metadata)
        self.assertTrue(res.success)
    #неуспешное удаление при наличии поддиректорий
    def test_10_fail_delete_directory_with_subdirectories(self):
        parent = self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="ParentSafe", parent_id=""), metadata=self.metadata)
        self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="ChildSafe", parent_id=parent.id), metadata=self.metadata)
        res = self.catalog_stub.DeleteDirectory(catalog_pb2.DeleteDirectoryRequest(id=parent.id), metadata=self.metadata)
        self.assertFalse(res.success) 
    #передвинуть директорию в root
    def test_11_move_directory_to_root(self):
        a = self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="A_Root", parent_id=""), metadata=self.metadata)
        b = self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="B_Root", parent_id=a.id), metadata=self.metadata)
        updated = self.catalog_stub.UpdateDirectory(
            catalog_pb2.UpdateDirectoryRequest(id=b.id, parent_id=""),
            metadata=self.metadata
        )
        self.assertEqual(updated.parent_id, "")
    #частичное обновление директории
    def test_12_update_directory_partial(self):
        d = self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="Original", parent_id=""), metadata=self.metadata)
        upd = self.catalog_stub.UpdateDirectory(
            catalog_pb2.UpdateDirectoryRequest(id=d.id, name="Changed", parent_id="no_change"),
            metadata=self.metadata
        )
        self.assertEqual(upd.name, "Changed")

    #проверка на видимость/невидимость публичных файлов пользователем
    def test_13_public_vs_private_anonymous(self):
        root = self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="ACL_Root", is_public=True), metadata=self.meta_owner)
        pub = self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="ACL_Pub", parent_id=root.id, is_public=True), metadata=self.meta_owner)
        priv = self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="ACL_Priv", parent_id=root.id, is_public=False), metadata=self.meta_owner)

        content = self.catalog_stub.GetDirectoryContent(catalog_pb2.GetDirectoryRequest(directory_id=root.id))
        names = [d.name for d in content.directories]
        
        self.assertIn("ACL_Pub", names)
        self.assertNotIn("ACL_Priv", names)
    #проверить невозможность доступа к закрытому проекту
    def test_14_stranger_access_denied(self):
        priv = self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="SecretBox", is_public=False), metadata=self.meta_owner)
        
        with self.assertRaises(grpc.RpcError) as cm:
            self.catalog_stub.GetDirectoryContent(
                catalog_pb2.GetDirectoryRequest(directory_id=priv.id),
                metadata=self.meta_stranger
            )
        self.assertEqual(cm.exception.code(), grpc.StatusCode.PERMISSION_DENIED)
    #проверка прав читателя
    def test_15_reader_rights(self):
        """ACL: Читатель может открыть, но не менять."""
        d = self.catalog_stub.CreateDirectory(
            catalog_pb2.CreateDirectoryRequest(name="ReadOnly", allowed_readers=[self.id_reader]),
            metadata=self.meta_owner
        )
        # Read OK
        self.catalog_stub.GetDirectoryContent(catalog_pb2.GetDirectoryRequest(directory_id=d.id), metadata=self.meta_reader)
        
        # Write Fail
        with self.assertRaises(grpc.RpcError) as cm:
            self.catalog_stub.UpdateDirectory(
                catalog_pb2.UpdateDirectoryRequest(id=d.id, name="Hacked"),
                metadata=self.meta_reader
            )
        self.assertEqual(cm.exception.code(), grpc.StatusCode.PERMISSION_DENIED)
    #возможность приглашения разрешенной группе
    def test_16_writer_invite_rights(self):
        """ACL: Писатель может менять имя и приглашать других."""
        d = self.catalog_stub.CreateDirectory(
            catalog_pb2.CreateDirectoryRequest(name="Wiki", allowed_writers=[self.id_writer]),
            metadata=self.meta_owner
        )

        # Читатель пытается зайти ДО получения прав (должен получить отказ)
        with self.assertRaises(grpc.RpcError) as cm:
            self.catalog_stub.GetDirectoryContent(
                catalog_pb2.GetDirectoryRequest(directory_id=d.id), 
                metadata=self.meta_reader
            )
        self.assertEqual(cm.exception.code(), grpc.StatusCode.PERMISSION_DENIED)

        # Писатель выдает права читателю
        self.catalog_stub.UpdateDirectory(
            catalog_pb2.UpdateDirectoryRequest(id=d.id, parent_id="no_change", allowed_readers=[self.id_reader]),
            metadata=self.meta_writer
        )
        
        # ПРОВЕРКА: Читатель заходит ПОСЛЕ получения прав (должно быть успешно)
        self.catalog_stub.GetDirectoryContent(catalog_pb2.GetDirectoryRequest(directory_id=d.id), metadata=self.meta_reader)

    #запрос на запись без токена
    def test_17_auth_missing_token_write(self):
        with self.assertRaises(grpc.RpcError) as cm:
            self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="Hacker"))
        self.assertEqual(cm.exception.code(), grpc.StatusCode.UNAUTHENTICATED)
    #запрос с плохим токеном
    def test_18_auth_invalid_token(self):
        bad_meta = (('authorization', 'Bearer fake-token'),)
        with self.assertRaises(grpc.RpcError) as cm:
            self.catalog_stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(name="Hacker"), metadata=bad_meta)
        self.assertEqual(cm.exception.code(), grpc.StatusCode.UNAUTHENTICATED)

    # ==========================================
    # часть 2: стресс тесты
    # ==========================================
    #оздание 100 директорий
    def test_19_stress_create_directories(self):
        print("\n[Stress] Creating 100 directories concurrently...")
        count = 100
        
        def create_one(i):
            return self.catalog_stub.CreateDirectory(
                catalog_pb2.CreateDirectoryRequest(name=f"StressDir_{i}", parent_id=""),
                metadata=self.meta_owner
            )

        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures_list = [executor.submit(create_one, i) for i in range(count)]
            results = [f.result() for f in concurrent.futures.as_completed(futures_list)]
        
        duration = time.time() - start_time
        print(f"   -> Finished in {duration:.4f}s")
        self.assertEqual(len(results), count)
    #создание директорий с вложенностью в 50
    def test_20_stress_deep_nesting(self):
        print("\n[Stress] Creating 50 nested directory levels...")
        depth = 50
        current_parent = ""
        ids = []
        
        start_time = time.time()
        for i in range(depth):
            res = self.catalog_stub.CreateDirectory(
                catalog_pb2.CreateDirectoryRequest(name=f"Level_{i}", parent_id=current_parent),
                metadata=self.meta_owner
            )
            current_parent = res.id
            ids.append(res.id)
            
        duration = time.time() - start_time
        print(f"   -> Finished in {duration:.4f}s")
        
        for dir_id in reversed(ids):
            self.catalog_stub.DeleteDirectory(
                catalog_pb2.DeleteDirectoryRequest(id=dir_id), 
                metadata=self.meta_owner
            )

if __name__ == '__main__':
    unittest.main()
