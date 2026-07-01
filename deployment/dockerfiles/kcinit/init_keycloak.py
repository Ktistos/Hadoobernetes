import os

from keycloak import KeycloakAdmin

KEYCLOAK_URL = os.environ.get("KEYCLOAK_URL", "http://keycloak:8080")
ADMIN_USERNAME = os.environ["KC_BOOTSTRAP_ADMIN_USERNAME"]
ADMIN_PASSWORD = os.environ["KC_BOOTSTRAP_ADMIN_PASSWORD"]

REALM = "hadoobernetes"
CLIENT_ID = "mapreduce-client"
ADMIN_ROLE = "mapreduce-admin"
TEST_USER = {
    "username": "testuser",
    "email": "testuser@hadoobernetes.local",
    "password": "test",
}


def main():
    admin = KeycloakAdmin(
        server_url=KEYCLOAK_URL,
        username=ADMIN_USERNAME,
        password=ADMIN_PASSWORD,
        realm_name="master",
    )

    # Create realm if it doesn't exist
    print(f"Creating realm '{REALM}'...")
    existing_realms = [r["realm"] for r in admin.get_realms()]
    if REALM not in existing_realms:
        admin.create_realm(payload={"realm": REALM, "enabled": True, "displayName": "hadoobernetes"})
    else:
        print("  Already exists, skipping.")
    print("  Done.")

    # Reinitialize admin scoped to the new realm, authenticating via master
    admin = KeycloakAdmin(
        server_url=KEYCLOAK_URL,
        username=ADMIN_USERNAME,
        password=ADMIN_PASSWORD,
        realm_name=REALM,
        user_realm_name="master",
    )

    # Create client if it doesn't exist
    print(f"Creating client '{CLIENT_ID}'...")
    existing_clients = [c["clientId"] for c in admin.get_clients()]
    if CLIENT_ID not in existing_clients:
        admin.create_client(payload={
            "clientId": CLIENT_ID,
            "enabled": True,
            "publicClient": True,
            "directAccessGrantsEnabled": True,
            "standardFlowEnabled": False,
        })
    else:
        print("  Already exists, skipping.")
    print("  Done.")

    client_uuid = admin.get_client_id(CLIENT_ID)
    if client_uuid is None:
        raise RuntimeError(f"Client '{CLIENT_ID}' not found after initialization")

    print(f"Creating client role '{ADMIN_ROLE}'...")
    admin.create_client_role(
        client_uuid,
        payload={"name": ADMIN_ROLE},
        skip_exists=True,
    )
    admin_role = admin.get_client_role(client_uuid, ADMIN_ROLE)
    print("  Done.")

    # Create user if it doesn't exist
    print(f"Creating user '{TEST_USER['username']}'...")
    existing_users = [u["username"] for u in admin.get_users()]
    if TEST_USER["username"] not in existing_users:
        user_id = admin.create_user(payload={
            "username": TEST_USER["username"],
            "email": TEST_USER["email"],
            "firstName": TEST_USER["username"],
            "lastName": "User",
            "enabled": True,
            "emailVerified": True,
            "credentials": [
                {
                    "type": "password",
                    "value": TEST_USER["password"],
                    "temporary": False,
                }
            ],
        })
    else:
        user_id = admin.get_user_id(TEST_USER["username"])
        print("  Already exists, skipping.")
    print(f"  Done. (id: {user_id})")

    print(f"Assigning client role '{ADMIN_ROLE}' to '{TEST_USER['username']}'...")
    existing_role_names = {
        role["name"] for role in admin.get_client_roles_of_user(user_id, client_uuid)
    }
    if ADMIN_ROLE not in existing_role_names:
        admin.assign_client_role(user_id, client_uuid, admin_role)
    else:
        print("  Already assigned, skipping.")
    print("  Done.")

    print("\nKeycloak initialized successfully.")


if __name__ == "__main__":
    main()
