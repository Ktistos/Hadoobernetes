"""
Authentication manager for the Hadoobernetes CLI.

Handles communication with the Keycloak service to securely exchange 
credentials for JSON Web Tokens (JWT) and caches the token locally 
in the user's home directory for seamless subsequent commands.
"""

import os
import json
import requests
from pathlib import Path
import base64

KEYCLOAK_URL = os.getenv("KEYCLOAK_URL", "http://kc.minikube.local")
REALM = os.getenv("KEYCLOAK_REALM", "hadoobernetes")
CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID", "mapreduce-client")

# Define where to store the token locally (e.g., ~/.hadoobernetes/auth.json)
CONFIG_DIR = Path.home() / ".hadoobernetes"
TOKEN_FILE = CONFIG_DIR / "auth.json"

def login(username: str, password: str) -> None:
    """
    Authenticates the user against Keycloak and saves the resulting tokens locally.
    
    Args:
        username (str): The user's Keycloak username.
        password (str): The user's Keycloak password.
        
    Raises:
        Exception: If the authentication request fails or returns invalid credentials.
    """
    url = f"{KEYCLOAK_URL}/realms/{REALM}/protocol/openid-connect/token"
    payload = {
        "client_id": CLIENT_ID,
        "username": username,
        "password": password,
        "grant_type": "password",
    }
    
    response = requests.post(url, data=payload)
    
    if response.status_code != 200:
        # Attempt to parse the JSON error message gracefully
        try:
            error_data = response.json()
            error_msg = error_data.get("error_description", response.text)
        except ValueError:
            # Fallback if Keycloak returns a non-JSON 500 error
            error_msg = response.text
            
        raise Exception(f"Login failed: {error_msg}")
        
    # Ensure directory exists and save token
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(TOKEN_FILE, "w") as f:
        json.dump(response.json(), f)

def get_access_token() -> str:
    """
    Retrieves the cached Keycloak access token from the local filesystem.
    
    Returns:
        str: The raw JWT access token string.
        
    Raises:
        Exception: If the user is not logged in (token file is missing).
    """
    if not TOKEN_FILE.exists():
        raise Exception("You are not logged in. Please run `hadoob login` first.")
        
    with open(TOKEN_FILE, "r") as f:
        data = json.load(f)
        return data.get("access_token")

def get_current_user_id() -> str:
    """
    Retrieves the user ID (subject) from the cached Keycloak access token.
    
    Returns:
        str: The user's Keycloak subject ID.
        
    Raises:
        Exception: If decoding fails or the subject is missing.
    """
    token = get_access_token()
    try:
        # The payload is the second segment of the JWT
        payload_b64 = token.split(".")[1]
        
        # Add padding required by base64 decoding
        payload_b64 += "=" * (4 - len(payload_b64) % 4)
        claims = json.loads(base64.b64decode(payload_b64))
        
        user_id = claims.get("sub")
        if not user_id:
            raise Exception("Token payload is missing the 'sub' claim.")
            
        return user_id
    except Exception as e:
        raise Exception(f"Failed to parse user ID from token: {e}")