import unittest
import grpc
import catalog_pb2
import catalog_pb2_grpc
import time

class TestCatalogService(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up the connection once for all tests."""
        print("Connecting to gRPC server...")
        cls.channel = grpc.insecure_channel('localhost:50051')
        cls.stub = catalog_pb2_grpc.CatalogServiceStub(cls.channel)

    @classmethod
    def tearDownClass(cls):
        """Close the channel after tests."""
        cls.channel.close()

    def create_dir(self, name, parent_id=""):
        """Helper to create a directory."""
        return self.stub.CreateDirectory(catalog_pb2.CreateDirectoryRequest(
            name=name,
            parent_id=parent_id
        ))

    def test_01_directory_lifecycle(self):
        """Test Creating, Renaming, and Deleting a directory."""
        print("\n--- Test 1: Directory Lifecycle ---")
        
        # 1. Create
        d = self.create_dir("Temp Folder")
        self.assertIsNotNone(d.id)
        self.assertEqual(d.name, "Temp Folder")
        print(f"Created: {d.id}")

        # 2. Update (Rename)
        updated = self.stub.UpdateDirectory(catalog_pb2.UpdateDirectoryRequest(
            id=d.id,
            name="Renamed Folder",
            parent_id="no_change"
        ))
        self.assertEqual(updated.name, "Renamed Folder")
        print("Renamed to 'Renamed Folder'")

        # 3. Delete
        delete_res = self.stub.DeleteDirectory(catalog_pb2.DeleteDirectoryRequest(id=d.id))
        self.assertTrue(delete_res.success)
        print("Deleted successfully")

    def test_02_nested_directories_and_constraints(self):
        """Test parent/child relationships and deletion constraints."""
        print("\n--- Test 2: Nested Directories & Constraints ---")

        # 1. Create Parent and Child
        parent = self.create_dir("Parent")
        child = self.create_dir("Child", parent_id=parent.id)
        
        self.assertEqual(child.parent_id, parent.id)
        print(f"Created structure: {parent.name} -> {child.name}")

        # 2. Verify Content listing
        content = self.stub.GetDirectoryContent(catalog_pb2.GetDirectoryRequest(directory_id=parent.id))
        # Verify 'Child' is in the list of directories inside 'Parent'
        found_child = any(d.id == child.id for d in content.directories)
        self.assertTrue(found_child, "Child folder should appear in Parent's content")

        # 3. Attempt to Delete Parent (Should Fail because it's not empty)
        del_res = self.stub.DeleteDirectory(catalog_pb2.DeleteDirectoryRequest(id=parent.id))
        self.assertFalse(del_res.success, "Should fail to delete non-empty directory")
        print("Correctly prevented deletion of non-empty directory")

        # 4. Cleanup (Delete Child first, then Parent)
        self.stub.DeleteDirectory(catalog_pb2.DeleteDirectoryRequest(id=child.id))
        del_res_2 = self.stub.DeleteDirectory(catalog_pb2.DeleteDirectoryRequest(id=parent.id))
        self.assertTrue(del_res_2.success)
        print("Cleanup successful")

    def test_03_file_operations(self):
        """Test Registering, Listing, and Deleting files."""
        print("\n--- Test 3: File Operations ---")

        # 1. Setup folder
        folder = self.create_dir("File Test Folder")
        
        # 2. Register File
        f_obj = self.stub.RegisterFile(catalog_pb2.RegisterFileRequest(
            name="test_doc.pdf",
            parent_directory_id=folder.id,
            external_file_id="ext-12345"
        ))
        self.assertEqual(f_obj.name, "test_doc.pdf")
        self.assertEqual(f_obj.external_file_id, "ext-12345")
        print(f"Registered file: {f_obj.id}")

        # 3. List content to verify file is there
        content = self.stub.GetDirectoryContent(catalog_pb2.GetDirectoryRequest(directory_id=folder.id))
        found_file = any(f.id == f_obj.id for f in content.files)
        self.assertTrue(found_file, "File should appear in directory listing")

        # 4. Delete File
        del_res = self.stub.DeleteFile(catalog_pb2.DeleteFileRequest(id=f_obj.id))
        self.assertTrue(del_res.success)
        print("File deleted")

        # 5. Cleanup folder
        self.stub.DeleteDirectory(catalog_pb2.DeleteDirectoryRequest(id=folder.id))

    def test_04_move_directory(self):
        """Test moving a directory to a new parent."""
        print("\n--- Test 4: Moving Directory ---")
        
        # Structure: Folder A, Folder B. Move B into A.
        folder_a = self.create_dir("Folder A")
        folder_b = self.create_dir("Folder B") # Currently at root

        # Update Folder B to have Folder A as parent
        self.stub.UpdateDirectory(catalog_pb2.UpdateDirectoryRequest(
            id=folder_b.id,
            parent_id=folder_a.id # Moving it here
        ))

        # Verify the move
        # 1. Check Folder B's parent
        # (We don't have a GetDirectory method in proto, so we check content of A)
        content_a = self.stub.GetDirectoryContent(catalog_pb2.GetDirectoryRequest(directory_id=folder_a.id))
        found_b_in_a = any(d.id == folder_b.id for d in content_a.directories)
        self.assertTrue(found_b_in_a, "Folder B should now be inside Folder A")
        print("Folder B successfully moved into Folder A")

        # Cleanup
        self.stub.DeleteDirectory(catalog_pb2.DeleteDirectoryRequest(id=folder_b.id))
        self.stub.DeleteDirectory(catalog_pb2.DeleteDirectoryRequest(id=folder_a.id))

if __name__ == '__main__':
    # Verbosity=2 gives detailed output for each test
    unittest.main(verbosity=2)
