"""
Connexion à Neon PostGIS avec pool de connexions.
"""
import os
from psycopg_pool import ConnectionPool
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL non définie. "
        "Copie api/.env.example en api/.env et configure ta connection string Neon."
    )

connection_pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=1,
    max_size=5,
)


def get_conn():
    return connection_pool.getconn()


def release_conn(conn):
    connection_pool.putconn(conn)


def query(sql, params=None):
    with connection_pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description:
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                return [dict(zip(columns, row)) for row in rows]
            return []


def execute(sql, params=None):
    with connection_pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            if cur.description:
                columns = [desc[0] for desc in cur.description]
                row = cur.fetchone()
                return dict(zip(columns, row)) if row else None
            return None


def query_single(sql, params=None):
    with connection_pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            result = cur.fetchone()
            return result[0] if result else None
