import psycopg2

# Requires: kubectl port-forward -n mapreduce svc/postgres 5432:5432
HOST = "localhost"
PORT = 5432
DB = "mapreduce"
USER = "admin"
PASSWORD = "admin"

EXPECTED_SCHEMAS = {"mapreduce", "keycloak"}
EXPECTED_TABLES = {
    "mapreduce.jobs",
    "mapreduce.job_config",
    "mapreduce.map_tasks",
    "mapreduce.reduce_tasks",
}
EXPECTED_ENUMS = {
    "mapreduce.job_status",
    "mapreduce.task_status",
}


def main():
    # 1. Connect
    print("1. Connecting to PostgreSQL...")
    conn = psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASSWORD)
    cur = conn.cursor()
    cur.execute("SELECT version()")
    version = cur.fetchone()[0]
    print(f"   Connected. {version.split(',')[0]}")

    # 2. Check schemas
    print("\n2. Checking schemas...")
    cur.execute("SELECT schema_name FROM information_schema.schemata")
    schemas = {row[0] for row in cur.fetchall()}
    for expected in EXPECTED_SCHEMAS:
        assert expected in schemas, f"Missing schema: {expected}"
        print(f"   {expected} ✓")

    # 3. Check tables
    print("\n3. Checking tables...")
    cur.execute("""
        SELECT table_schema || '.' || table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
    """)
    tables = {row[0] for row in cur.fetchall()}
    for expected in EXPECTED_TABLES:
        assert expected in tables, f"Missing table: {expected}"
        print(f"   {expected} ✓")

    # 4. Check enums
    print("\n4. Checking enums...")
    cur.execute("""
        SELECT n.nspname || '.' || t.typname
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typtype = 'e'
    """)
    enums = {row[0] for row in cur.fetchall()}
    for expected in EXPECTED_ENUMS:
        assert expected in enums, f"Missing enum: {expected}"
        print(f"   {expected} ✓")

    cur.close()
    conn.close()
    print("\nAll checks passed.")


if __name__ == "__main__":
    main()
