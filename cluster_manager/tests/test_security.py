import pytest
import jwt
import asyncio
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from unittest.mock import AsyncMock
import security

def test_get_keycloak_public_key(monkeypatch):
    """Test fetching JWKS from Keycloak."""
    security._public_key = None
    
    class MockResponse:
        status_code = 200
        def json(self):
            return {"keys": [{"kty": "RSA", "use": "sig", "n": "mock", "e": "mock"}]}
            
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value.get.return_value = MockResponse()
    monkeypatch.setattr("httpx.AsyncClient", lambda: mock_client)
    monkeypatch.setattr("jwt.algorithms.RSAAlgorithm.from_jwk", lambda k: "MockRSAPublicKey")
    
    key = asyncio.run(security.get_keycloak_public_key())
    assert key == "MockRSAPublicKey"

def test_verify_token_valid(monkeypatch):
    monkeypatch.setattr(security, "get_keycloak_public_key", AsyncMock(return_value="mock_key"))
    monkeypatch.setattr(jwt, "decode", lambda *args, **kwargs: {"sub": "user-123"})
    
    creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="valid_token")
    payload = asyncio.run(security.verify_token(creds))
    assert payload["sub"] == "user-123"

def test_verify_token_expired(monkeypatch):
    monkeypatch.setattr(security, "get_keycloak_public_key", AsyncMock(return_value="mock_key"))
    
    def mock_decode(*args, **kwargs):
        raise jwt.ExpiredSignatureError()
        
    monkeypatch.setattr(jwt, "decode", mock_decode)
    
    creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="expired")
    with pytest.raises(HTTPException) as exc:
        asyncio.run(security.verify_token(creds))
        
    assert exc.value.status_code == 401
    assert "expired" in exc.value.detail

def test_get_current_user():
    user_id = asyncio.run(security.get_current_user({"sub": "user-123"}))
    assert user_id == "user-123"
    
    with pytest.raises(HTTPException) as exc:
        asyncio.run(security.get_current_user({"email": "test@test.com"}))
        
    assert exc.value.status_code == 401

def test_require_admin_accepts_realm_role(monkeypatch):
    monkeypatch.setattr(security, "ADMIN_ROLES", {"admin", "mapreduce-admin"})

    user_id = asyncio.run(
        security.require_admin({"sub": "user-123", "realm_access": {"roles": ["admin"]}})
    )

    assert user_id == "user-123"

def test_require_admin_accepts_client_role(monkeypatch):
    monkeypatch.setattr(security, "ADMIN_ROLES", {"admin", "mapreduce-admin"})

    user_id = asyncio.run(
        security.require_admin(
            {
                "sub": "user-123",
                "resource_access": {"mapreduce-client": {"roles": ["mapreduce-admin"]}},
            }
        )
    )

    assert user_id == "user-123"

def test_require_admin_rejects_non_admin(monkeypatch):
    monkeypatch.setattr(security, "ADMIN_ROLES", {"admin", "mapreduce-admin"})

    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            security.require_admin(
                {"sub": "user-123", "realm_access": {"roles": ["user"]}}
            )
        )

    assert exc.value.status_code == 403
