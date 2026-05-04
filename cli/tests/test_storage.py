"""
Unit tests for the MinIO storage orchestrator.
"""
import pytest
import os
import storage

def test_upload_file_success(monkeypatch, tmp_path):
    """Test that files are successfully uploaded and format the correct MinIO path."""
    # Create a dummy file to upload
    test_file = tmp_path / "dummy_data.txt"
    test_file.write_text("Sample input data for map reduce.")
    
    # Mock the MinIO client behavior
    class MockMinioClient:
        def bucket_exists(self, bucket_name):
            return True
            
        def fput_object(self, bucket_name, object_name, file_path):
            # Intercept the upload to ensure arguments are correct
            assert bucket_name == storage.BUCKET
            assert "users/staged/dummy_data.txt" in object_name
            assert file_path == str(test_file)

    monkeypatch.setattr(storage, "get_client", lambda: MockMinioClient())
    
    # Execute
    result_url = storage.upload_file(str(test_file), "users/staged")
    
    # Assert the returned URL is formatted correctly
    assert result_url == f"minio://{storage.BUCKET}/users/staged/dummy_data.txt"

def test_upload_file_not_found():
    """Test that uploading a non-existent file raises a FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        storage.upload_file("/path/to/nowhere.txt", "users/staged")