"""
Unit tests for the CLI authentication manager.
"""
import pytest
import json
from pathlib import Path
import auth

def test_login_success(monkeypatch, tmp_path):
    """Test that a successful login correctly writes the token to the local config file."""
    # Reroute the config directory to a temporary testing folder
    test_config_dir = tmp_path / ".hadoobernetes"
    test_token_file = test_config_dir / "auth.json"
    
    monkeypatch.setattr(auth, "CONFIG_DIR", test_config_dir)
    monkeypatch.setattr(auth, "TOKEN_FILE", test_token_file)
    
    # Mock the Keycloak HTTP response
    class MockResponse:
        status_code = 200
        text = "OK"
        def json(self):
            return {"access_token": "mocked_jwt_token", "refresh_token": "mocked_refresh"}
            
    monkeypatch.setattr("requests.post", lambda *args, **kwargs: MockResponse())
    
    # Execute
    auth.login("testuser", "testpass")
    
    # Assert token file was created and contains the right token
    assert test_token_file.exists()
    assert auth.get_access_token() == "mocked_jwt_token"

def test_get_access_token_fails_when_not_logged_in(monkeypatch, tmp_path):
    """Test that fetching a token without logging in raises a helpful exception."""
    monkeypatch.setattr(auth, "TOKEN_FILE", tmp_path / "nonexistent.json")
    
    with pytest.raises(Exception) as exc_info:
        auth.get_access_token()
        
    assert "You are not logged in" in str(exc_info.value)