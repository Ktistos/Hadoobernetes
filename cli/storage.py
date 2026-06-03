"""
Storage orchestrator for the Hadoobernetes CLI.
Provides direct interaction with the MinIO cluster to handle the staging
of user input data and executable code prior to job submission.
"""
import os
from minio import Minio
from minio.error import S3Error
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
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
    client.fput_object(BUCKET, object_name, local_path)
    return f"minio://{BUCKET}/{object_name}"
def download_file(remote_path: str, local_path: str) -> None:
    """
    Downloads a file from the MinIO object storage cluster.
    Args:
        remote_path (str): The specific path inside the bucket.
        local_path (str): The local destination path to save the file.
    """
    client = get_client()
    try:
        client.fget_object(BUCKET, remote_path, local_path)
    except S3Error as e:
        raise Exception(f"{e.code}: {e.message}")
    except Exception as e:
        raise Exception(str(e))
def download_prefix(remote_prefix: str, local_dir: str) -> list[str]:
    """
    Downloads every object under a MinIO prefix into a local directory.
    Args:
        remote_prefix (str): The prefix (folder) inside the bucket to fetch.
        local_dir (str): Local destination directory (created if absent).
    Returns:
        list[str]: The local file paths that were written.
    Raises:
        Exception: If listing fails. Individual object failures are raised too.
    """
    client = get_client()
    os.makedirs(local_dir, exist_ok=True)
    written: list[str] = []
    try:
        objects = list(client.list_objects(BUCKET, prefix=remote_prefix, recursive=True))
    except S3Error as e:
        raise Exception(f"{e.code}: {e.message}")
    for obj in objects:
        if obj.object_name.endswith("/"):
            continue
        filename = os.path.basename(obj.object_name)
        dest = os.path.join(local_dir, filename)
        client.fget_object(BUCKET, obj.object_name, dest)
        written.append(dest)
    return written