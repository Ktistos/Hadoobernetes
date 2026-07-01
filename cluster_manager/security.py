"""
Authentication and security logic for the Cluster Manager.

This module handles JWT validation locally using Keycloak's public keys. It ensures
that endpoints are protected and can correctly identify the requesting user.
"""

import os
import httpx
import jwt
from fastapi import Request, HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
from jwt.algorithms import RSAAlgorithm

security = HTTPBearer()

KEYCLOAK_URL = os.getenv("KEYCLOAK_URL", "http://kc.minikube.local")
REALM = os.getenv("KEYCLOAK_REALM", "hadoobernetes")
CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID", "mapreduce-client")
ADMIN_ROLES = {
    role.strip()
    for role in os.getenv("ADMIN_ROLES", "admin,mapreduce-admin").split(",")
    if role.strip()
}

_public_key: RSAPublicKey | None = None

async def get_keycloak_public_key() -> RSAPublicKey:
    """
    Fetches and caches the RSA public key from the Keycloak server's JWKS endpoint.
    
    Returns:
        RSAPublicKey: The cryptographic public key used to verify JWT signatures.
        
    Raises:
        HTTPException: If the Keycloak server is unreachable or valid keys are not found.
    """
    global _public_key
    if _public_key:
        return _public_key

    jwks_url = f"{KEYCLOAK_URL}/realms/{REALM}/protocol/openid-connect/certs"
    async with httpx.AsyncClient() as client:
        response = await client.get(jwks_url)
        if response.status_code != 200:
            raise HTTPException(status_code=500, detail="Unable to fetch Keycloak public keys")
        
        jwks = response.json()
        
        for key in jwks.get("keys", []):
            if key.get("kty") == "RSA" and key.get("use") == "sig":
                _public_key = RSAAlgorithm.from_jwk(key)
                return _public_key
                
    raise HTTPException(status_code=500, detail="Valid RSA public key not found in Keycloak JWKS")

async def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)) -> dict:
    """
    FastAPI dependency that decodes and validates the incoming JSON Web Token.
    
    Args:
        credentials (HTTPAuthorizationCredentials): The injected Bearer token from the request header.
        
    Returns:
        dict: The decoded token payload containing user claims and metadata.
        
    Raises:
        HTTPException: If the token is expired, tampered with, or mathematically invalid.
    """
    token = credentials.credentials
    public_key = await get_keycloak_public_key()
    
    try:
        decoded_token = jwt.decode(
            token,
            public_key,
            algorithms=["RS256"],
            audience="account",
            options={"verify_aud": False}
        )
        return decoded_token
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")

async def get_current_user(token_payload: dict = Security(verify_token)) -> str:
    """
    FastAPI dependency that extracts the subject (user ID) from a verified token.
    
    Args:
        token_payload (dict): The verified JWT payload provided by verify_token.
        
    Returns:
        str: The Keycloak Subject ID representing the authenticated user.
        
    Raises:
        HTTPException: If the token payload is missing the 'sub' claim.
    """
    user_id = token_payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Subject (sub) not found in token")
    return user_id

def _token_roles(token_payload: dict) -> set[str]:
    roles = set(token_payload.get("realm_access", {}).get("roles", []))
    client_roles = (
        token_payload.get("resource_access", {})
        .get(CLIENT_ID, {})
        .get("roles", [])
    )
    roles.update(client_roles)
    return roles

async def require_admin(token_payload: dict = Security(verify_token)) -> str:
    """
    FastAPI dependency that allows access only to tokens carrying one of the
    accepted admin roles.
    """
    user_id = token_payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Subject (sub) not found in token")

    if _token_roles(token_payload) & ADMIN_ROLES:
        return user_id

    raise HTTPException(status_code=403, detail="Admin access required")
