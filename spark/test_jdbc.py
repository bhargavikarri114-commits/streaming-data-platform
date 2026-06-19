# test_jdbc.py

import psycopg2

for db in ["postgres", "streaming_db"]:
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database=db,
            user="postgres",
            password="Pmnbvcxz@1"
        )
        print(f"SUCCESS: {db}")
        conn.close()
    except Exception as e:
        print(f"FAILED: {db}")
        print(e)