"""
HTTP API Client for communicating with the Cluster Manager.

Wraps the requests library to send authenticated payloads to the FastAPI 
Cluster Manager service for job lifecycle management.
"""

import os
import requests
from typing import Dict, Any

from auth import get_access_token

CLUSTER_MANAGER_URL = os.getenv("CLUSTER_MANAGER_URL", "http://localhost:8000")

def _get_headers() -> Dict[str, str]:
    """
    Constructs the HTTP headers required for requests, embedding the JWT.
    
    Returns:
        Dict[str, str]: The headers dictionary including the Authorization Bearer token.
    """
    token = get_access_token()
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

def submit_job(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Submits a prepared Map-Reduce job payload to the Cluster Manager.
    
    Args:
        payload (Dict[str, Any]): The validated job configuration dictionary.
        
    Returns:
        Dict[str, Any]: The JSON response from the server containing the new job_id.
        
    Raises:
        Exception: If the server rejects the request (e.g., validation or internal error).
    """
    url = f"{CLUSTER_MANAGER_URL}/submit_job"
    response = requests.post(url, json=payload, headers=_get_headers())
    
    if response.status_code != 200:
        raise Exception(f"Failed to submit job: {response.text}")
        
    return response.json()

def get_status(job_id: str) -> Dict[str, Any]:
    """
    Fetches the real-time execution status of a specific job.
    
    Args:
        job_id (str): The UUID of the job to query.
        
    Returns:
        Dict[str, Any]: The JSON status response including task completion counts.
    """
    url = f"{CLUSTER_MANAGER_URL}/job_status/{job_id}"
    response = requests.get(url, headers=_get_headers())
    
    if response.status_code != 200:
        raise Exception(f"Failed to get status: {response.text}")
        
    return response.json()

def abort_job(job_id: str) -> Dict[str, Any]:
    """
    Sends a request to manually terminate an active job.
    
    Args:
        job_id (str): The UUID of the job to abort.
        
    Returns:
        Dict[str, Any]: The confirmation JSON response from the server.
    """
    url = f"{CLUSTER_MANAGER_URL}/abort_job/{job_id}"
    response = requests.post(url, headers=_get_headers())
    
    if response.status_code != 200:
        raise Exception(f"Failed to abort job: {response.text}")
        
    return response.json()