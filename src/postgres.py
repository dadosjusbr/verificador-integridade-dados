import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    try:
        conn = psycopg2.connect(
            database=os.getenv('POSTGRES_DBNAME'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=5432,
        )
        if conn:
            return conn
    except:
        print("Connection to the PostgreSQL encountered and error.")
        os._exit(1)

def consultar_db(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    recset = cur.fetchall()
    registros = []
    for rec in recset:
        registros.append(rec)
    return registros