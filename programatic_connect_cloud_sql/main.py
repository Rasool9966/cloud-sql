# Install the Secret Manager client
# pip install google-cloud-secret-manager

# Install the MySQL Connector/Python driver
# pip install mysql-connector-python

import os
import io
from google.cloud import secretmanager
import mysql.connector
from mysql.connector import errorcode

PROJECT_ID      = "lyrical-respect-461712-k8"
INSTANCE        = "lyrical-respect-461712-k8:us-central1:gcp-mysql-dev"  # e.g. "proj:us-central1:dev-mysql"
DB_HOST         = "35.238.109.214"
DB_PORT         = 3306
DB_NAME         = None

# Secret names in Secret Manager
SECRET_DB_USER = "db_user"

SECRET_DB_PASS = "db-password"


def get_secret(project_id: str, secret_id: str, version: str = "latest") -> str:
    """
    Fetch the payload of the given secret version from Secret Manager.
    """
    client = secretmanager.SecretManagerServiceClient()
    name   = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def main():
    # 1) Retrieve credentials from Secret Manager
    try:
        db_user = get_secret(PROJECT_ID, SECRET_DB_USER)
        db_pass = get_secret(PROJECT_ID, SECRET_DB_PASS)
    except Exception as e:
        print(f"Failed to fetch secrets: {e}")
        return

    # 2) Connect to Cloud SQL via Auth Proxy listening on localhost:3306
    try:
        conn = mysql.connector.connect(
            user=db_user,
            password=db_pass,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME or None
        )
        print(f"✅ Connected to {DB_HOST}:{DB_PORT} as {db_user}")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Authentication error: check your secrets")
        else:
            print(f"Connection error: {err}")
        return

    cursor = conn.cursor()

    # 3) SHOW DATABASES
    print("\n--- Databases ---")
    cursor.execute("SHOW DATABASES;")
    for (db,) in cursor:
        print("  •", db)

    # 4) USE a database if none was specified
    if not DB_NAME:
        chosen_db = input("\nEnter a database to USE: ").strip()
        cursor.execute(f"USE `{chosen_db}`;")
        print(f"✅ Switched to database `{chosen_db}`")

    # 5) SHOW TABLES
    print("\n--- Tables ---")
    cursor.execute("SHOW TABLES;")
    tables = [row[0] for row in cursor]
    for tbl in tables:
        print("  •", tbl)

    # 6) SELECT sample rows from the first table
    if tables:
        sample_tbl = tables[0]
        print(f"\n--- Sample rows from `{sample_tbl}` ---")
        cursor.execute(f"SELECT * FROM `{sample_tbl}` LIMIT 5;")
        # print column headers
        cols = cursor.column_names
        print(" | ".join(cols))
        for row in cursor.fetchall():
            print(" | ".join(str(v) for v in row))

    # 7) Cleanup
    cursor.close()
    conn.close()
    print("\n Done.")

if __name__ == "__main__":
    main()