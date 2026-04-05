from keycloak import KeycloakOpenID
import base64
import json

KEYCLOAK_URL = "http://kc.minikube.local"
REALM = "hadoobernetes"
CLIENT_ID = "mapreduce-client"
USERNAME = "testuser"
PASSWORD = "test"


def main():
    kc = KeycloakOpenID(
        server_url=KEYCLOAK_URL,
        realm_name=REALM,
        client_id=CLIENT_ID,
    )

    # 1. Login
    print("1. Logging in...")
    token = kc.token(USERNAME, PASSWORD)
    access_token = token["access_token"]
    refresh_token = token["refresh_token"]
    print(f"   access_token  : {access_token[:60]}...")
    print(f"   refresh_token : {refresh_token[:60]}...")

    # 2. Userinfo
    print("\n2. Validating token via userinfo...")
    info = kc.userinfo(access_token)
    print(f"   Token is valid.")
    print(f"   username : {info.get('preferred_username')}")
    print(f"   email    : {info.get('email')}")
    print(f"   subject  : {info.get('sub')}")

    # 3. Decode token and check claims
    print("\n3. Decoding token and checking claims...")
    payload = access_token.split(".")[1]
    payload += "=" * (4 - len(payload) % 4)
    claims = json.loads(base64.b64decode(payload))
    print(f"   issuer    : {claims.get('iss')}")
    print(f"   audience  : {claims.get('aud')}")
    print(f"   client_id : {claims.get('azp')}")
    print(f"   policy    : {claims.get('policy', '(not set)')}")
    assert claims.get("azp") == CLIENT_ID, f"Expected client_id '{CLIENT_ID}', got '{claims.get('azp')}'"
    print(f"   client_id matches '{CLIENT_ID}' ✓")

    # 4. Token refresh
    print("\n4. Refreshing token...")
    refreshed = kc.refresh_token(refresh_token)
    new_access_token = refreshed["access_token"]
    print(f"   new access_token: {new_access_token[:60]}...")
    new_info = kc.userinfo(new_access_token)
    assert new_info.get("preferred_username") == USERNAME
    print(f"   Refreshed token is valid ✓")

    print("\nAll checks passed.")


if __name__ == "__main__":
    main()
