import os
import psycopg2


def get_connection():
    try:
        conn = psycopg2.connect(
            database=os.getenv('POSTGRES_DBNAME'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
        )
        if conn:
            return conn
    except Exception as e:
        print(f"Connection to the PostgreSQL encountered and error: {e}")
        os._exit(1)

def consultar_db(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    recset = cur.fetchall()
    registros = []
    for rec in recset:
        registros.append(rec)
    return registros

def run_db(conn, sql):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cur.close()
        return 1
    cur.close()