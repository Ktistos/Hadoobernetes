"""
Integration-style tests for the Click command-line interface.
"""
import pytest
from click.testing import CliRunner
from main import cli
import auth
import storage
import api_client

def test_login_command(monkeypatch):
    """Test the CLI login command successfully routes to the auth module."""
    # We mock the auth.login function so it doesn't actually make network calls
    monkeypatch.setattr(auth, "login", lambda u, p: None)
    
    runner = CliRunner()
    result = runner.invoke(cli, ["login", "--username", "testuser", "--password", "testpass"])
    
    assert result.exit_code == 0
    assert "Successfully logged in as testuser." in result.output

def test_submit_command(monkeypatch, tmp_path):
    """Test the full CLI submission flow: staging files and calling the API."""
    # Create mock files to pass the Click path validation
    input_file = tmp_path / "data.txt"
    input_file.write_text("data")
    code_file = tmp_path / "job.py"
    code_file.write_text("print('hello')")
    
    # Mock the heavy lifting
    monkeypatch.setattr(storage, "upload_file", lambda f, prefix: f"minio://mock/{prefix}/test")
    monkeypatch.setattr(api_client, "submit_job", lambda payload: {"job_id": "uuid-9999"})
    
    runner = CliRunner()
    result = runner.invoke(cli, [
        "submit", 
        "--mappers", "4", 
        "--reducers", "2", 
        "--input-file", str(input_file), 
        "--code", str(code_file)
    ])
    
    assert result.exit_code == 0
    assert "[*] Uploading input data to MinIO..." in result.output
    assert "[*] Submitting job to Cluster Manager..." in result.output
    assert "Success! Job ID: uuid-9999" in result.output

def test_status_command(monkeypatch):
    """Test the CLI status command outputs the parsed JSON nicely."""
    monkeypatch.setattr(api_client, "get_status", lambda jid: {"status": "completed", "completed_mappers_count": 4})
    
    runner = CliRunner()
    result = runner.invoke(cli, ["status", "uuid-9999"])
    
    assert result.exit_code == 0
    assert "Status for Job uuid-9999:" in result.output
    assert "'status': 'completed'" in result.output

def test_logout_command(monkeypatch, tmp_path):
    """Test the CLI logout command against the correct absolute path."""
    # 1. Create a mock absolute path in a temporary directory
    mock_token_file = tmp_path / "auth.json"
    
    # 2. Force the auth module to use our mock path during this test
    monkeypatch.setattr(auth, "TOKEN_FILE", mock_token_file)
    
    # 3. Simulate being logged in by actually creating the file
    mock_token_file.write_text("{}")
    
    runner = CliRunner()
    
    # First logout should succeed and delete our mock file
    result = runner.invoke(cli, ["logout"])
    assert result.exit_code == 0
    assert "Successfully logged out." in result.output
    assert not mock_token_file.exists()
    
    # Second logout should state we are not logged in
    result = runner.invoke(cli, ["logout"])
    assert result.exit_code == 0
    assert "You are not currently logged in." in result.output

def test_abort_command(monkeypatch):
    """Test the abort command happy path."""
    monkeypatch.setattr(api_client, "abort_job", lambda jid: {"message": "Job aborted."})
    runner = CliRunner()
    result = runner.invoke(cli, ["abort", "test-uuid"], input="y\n")
    assert result.exit_code == 0
    assert "Job aborted." in result.output

def test_upload_command(monkeypatch, tmp_path):
    """Test the upload command."""
    monkeypatch.setattr(auth, "get_current_user_id", lambda: "user-123")
    monkeypatch.setattr(storage, "upload_file", lambda src, dest: f"minio://mock/{dest}/file.txt")
    
    test_file = tmp_path / "data.txt"
    test_file.write_text("data")
    
    runner = CliRunner()
    result = runner.invoke(cli, ["upload", str(test_file), "inputs/"])
    assert result.exit_code == 0
    assert "Uploaded: minio://mock/users/user-123/inputs//file.txt" in result.output

def test_download_command(monkeypatch, tmp_path):
    """Test the download command."""
    monkeypatch.setattr(auth, "get_current_user_id", lambda: "user-123")
    
    def mock_download(remote, local):
        with open(local, "w") as f:
            f.write("mocked data")
            
    monkeypatch.setattr(storage, "download_file", mock_download)
    
    dest_file = tmp_path / "output.txt"
    runner = CliRunner()
    result = runner.invoke(cli, ["download", "outputs/result.txt", str(dest_file)])
    
    assert result.exit_code == 0
    assert "Successfully downloaded users/user-123/outputs/result.txt" in result.output
    assert dest_file.read_text() == "mocked data"