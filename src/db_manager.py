import os
import psycopg2
from dotenv import load_dotenv


load_dotenv()


def connect():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        # print("Conexão com PostgreSQL estabelecida.")
        return conn
    except Exception as error:
        print(f"Erro ao conectar no PostgreSQL: {error}")
        raise


def execute_query(conn, query, show=False):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()
        if show:
            print(f"Query executada com sucesso -> {query}")
    except Exception as error:
        if show:
            print(f"Erro ao executar query: {error}")
        conn.rollback()
        raise


def close_connection(conn):
    try:
        if conn:
            conn.close()
            print("Conexão encerrada.")
    except Exception as error:
        print(f"Erro ao fechar conexão: {error}")
