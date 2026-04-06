import io
import json

from keycloak import KeycloakOpenID
from minio import Minio
from minio.credentials import StaticProvider
from minio.error import S3Error
from minio import MinioAdmin

KEYCLOAK_URL = "http://kc.minikube.local"
REALM = "hadoobernetes"
CLIENT_ID = "mapreduce-client"
USERNAME = "testuser"
PASSWORD = "test"

MINIO_URL = "minio.minikube.local"
MINIO_ROOT_USER = "minioadmin"
MINIO_ROOT_PASSWORD = "minioadmin"
BUCKET = "mapreduce"

TEST_USER_ACCESS_KEY = "test-access-key"
TEST_USER_SECRET_KEY = "test-secret-key-1234"
TEST_POLICY_NAME = "test-user-policy"


def make_admin_client():
    return MinioAdmin(
        MINIO_URL,
        credentials=StaticProvider(MINIO_ROOT_USER, MINIO_ROOT_PASSWORD),
        secure=False,
    )


def make_s3_client(access_key, secret_key):
    return Minio(
        MINIO_URL,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )


def main():
    # ------------------------------------------------------------------ #
    # 1. Get Keycloak subject ID for testuser                             #
    # ------------------------------------------------------------------ #
    print("1. Fetching Keycloak subject ID for testuser...")
    kc = KeycloakOpenID(
        server_url=KEYCLOAK_URL,
        realm_name=REALM,
        client_id=CLIENT_ID,
    )
    token = kc.token(USERNAME, PASSWORD)
    info = kc.userinfo(token["access_token"])
    subject_id = info["sub"]
    print(f"   subject_id : {subject_id} ✓")

    user_prefix = f"users/{subject_id}"

    # ------------------------------------------------------------------ #
    # 2. Initialization checks (as admin)                                 #
    # ------------------------------------------------------------------ #
    print("\n2. Checking MinIO initialization...")
    admin_client = make_s3_client(MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)

    assert admin_client.bucket_exists(BUCKET), f"Bucket '{BUCKET}' does not exist"
    print(f"   bucket '{BUCKET}' exists ✓")

    for placeholder in ["users/.keep", "system/jobs/.keep"]:
        try:
            admin_client.stat_object(BUCKET, placeholder)
            print(f"   {placeholder} exists ✓")
        except S3Error as e:
            raise AssertionError(f"Missing placeholder: {placeholder}") from e

    # ------------------------------------------------------------------ #
    # 3. Anonymous access must be denied                                  #
    # ------------------------------------------------------------------ #
    print("\n3. Checking anonymous access is denied...")
    anon_client = Minio(MINIO_URL, secure=False)
    try:
        list(anon_client.list_objects(BUCKET))
        raise AssertionError("Anonymous list_objects should have been denied")
    except S3Error as e:
        assert e.code == "AccessDenied", f"Expected AccessDenied, got {e.code}"
        print(f"   Anonymous list rejected with AccessDenied ✓")

    # ------------------------------------------------------------------ #
    # 4. Create scoped MinIO user + policy for testuser                   #
    # ------------------------------------------------------------------ #
    print(f"\n4. Creating scoped MinIO user for subject '{subject_id}'...")
    admin = make_admin_client()

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:*"],
                "Resource": [
                    f"arn:aws:s3:::{BUCKET}/{user_prefix}/*",
                ],
            }
        ],
    }
    admin.add_policy(TEST_POLICY_NAME, json.dumps(policy))
    print(f"   Policy '{TEST_POLICY_NAME}' created ✓")

    admin.add_user(TEST_USER_ACCESS_KEY, TEST_USER_SECRET_KEY)
    admin.set_policy(TEST_POLICY_NAME, user=TEST_USER_ACCESS_KEY)
    print(f"   User '{TEST_USER_ACCESS_KEY}' created and policy attached ✓")

    # ------------------------------------------------------------------ #
    # 5. Authorized access: upload, download, delete                      #
    # ------------------------------------------------------------------ #
    print(f"\n5. Testing authorized access under '{user_prefix}/'...")
    user_client = make_s3_client(TEST_USER_ACCESS_KEY, TEST_USER_SECRET_KEY)

    test_object = f"{user_prefix}/inputs/test.txt"
    test_data = b"hello from testuser"

    user_client.put_object(BUCKET, test_object, io.BytesIO(test_data), len(test_data))
    print(f"   Upload  {test_object} ✓")

    response = user_client.get_object(BUCKET, test_object)
    downloaded = response.read()
    response.close()
    assert downloaded == test_data, "Downloaded content does not match uploaded content"
    print(f"   Download {test_object} ✓")

    user_client.remove_object(BUCKET, test_object)
    print(f"   Delete  {test_object} ✓")

    # ------------------------------------------------------------------ #
    # 6. Unauthorized access must be denied                               #
    # ------------------------------------------------------------------ #
    print("\n6. Testing unauthorized access is denied...")

    # Access system prefix
    try:
        response = user_client.get_object(BUCKET, "system/jobs/.keep")
        response.close()
        raise AssertionError("Access to system/jobs/.keep should have been denied")
    except S3Error as e:
        assert e.code == "AccessDenied", f"Expected AccessDenied, got {e.code}"
        print(f"   system/jobs/.keep rejected with AccessDenied ✓")

    # Access another user's prefix
    other_prefix = f"users/other-user-id/inputs/secret.txt"
    try:
        response = user_client.get_object(BUCKET, other_prefix)
        response.close()
        raise AssertionError("Access to another user's prefix should have been denied")
    except S3Error as e:
        assert e.code == "AccessDenied", f"Expected AccessDenied, got {e.code}"
        print(f"   {other_prefix} rejected with AccessDenied ✓")

    # Write to system prefix
    try:
        user_client.put_object(BUCKET, "system/jobs/injected.txt", io.BytesIO(b"x"), 1)
        raise AssertionError("Write to system/jobs/ should have been denied")
    except S3Error as e:
        assert e.code == "AccessDenied", f"Expected AccessDenied, got {e.code}"
        print(f"   Write to system/jobs/ rejected with AccessDenied ✓")

    # ------------------------------------------------------------------ #
    # 7. Cleanup                                                          #
    # ------------------------------------------------------------------ #
    print("\n7. Cleaning up...")
    admin.remove_user(TEST_USER_ACCESS_KEY)
    print(f"   User '{TEST_USER_ACCESS_KEY}' removed ✓")
    admin.remove_policy(TEST_POLICY_NAME)
    print(f"   Policy '{TEST_POLICY_NAME}' removed ✓")

    print("\nAll checks passed.")


if __name__ == "__main__":
    main()
