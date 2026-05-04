"""
Storage orchestrator for the Hadoobernetes CLI.

Provides direct interaction with the MinIO cluster to handle the staging 
of user input data and executable code prior to job submission.
"""

import os
from minio import Minio

MINIO_URL = os.getenv("MINIO_URL", "minio.minikube.local:80")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET = "mapreduce"

def get_client() -> Minio:
    """
    Initializes and returns the MinIO client using environment variables.
    
    Returns:
        Minio: An authenticated MinIO Python SDK client.
    """
    return Minio(
        MINIO_URL,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False,
    )

def upload_file(local_path: str, destination_prefix: str) -> str:
    """
    Uploads a local file to the MinIO object storage cluster.
    
    Args:
        local_path (str): The absolute or relative path to the local file.
        destination_prefix (str): The desired MinIO prefix (folder structure) to place the file in.
        
    Returns:
        str: The full minio:// URL where the file was successfully stored.
        
    Raises:
        FileNotFoundError: If the specified local file does not exist.
        Exception: If the MinIO upload fails.
    """
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Local file not found: {local_path}")

    file_size = os.path.getsize(local_path)
    file_name = os.path.basename(local_path)
    object_name = f"{destination_prefix.strip('/')}/{file_name}"
    
    client = get_client()
    
    # Ensure bucket exists (usually handled by init scripts, but safe to check)
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
        
    client.fput_object(BUCKET, object_name, local_path)
    
    return f"minio://{BUCKET}/{object_name}"