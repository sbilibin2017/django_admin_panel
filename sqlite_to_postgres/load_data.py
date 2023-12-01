import logging
import os
import pathlib
import sqlite3
from contextlib import closing

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
from psycopg2.extras import RealDictCursor

from db_settings import DSL
from src import conn_context
from transfer_to_psql import PostgresSaver, SQLiteExtractor


def load_from_sqlite(
        connection: sqlite3.Connection,
        pg_conn: _connection) -> None:
    """Основной метод загрузки данных из SQLite в Postgres"""
    sqlite_extractor = SQLiteExtractor(connection)    
    postgres_saver = PostgresSaver(pg_conn)
    data = sqlite_extractor.extract_movies()    
    postgres_saver.save_all_data(data)
    

if __name__ == '__main__':

    load_dotenv()

    logging.basicConfig(
        filename='example.log',
        encoding='utf-8',
        format='%(asctime)s %(message)s',
        level=logging.DEBUG
    )

    db = os.path.join(
        pathlib.Path(__file__).parent.absolute(),
        os.environ.get('DB_SQLITE_NAME'))

    with conn_context(db) as sqlite_conn, \
            closing(psycopg2.connect(**DSL, cursor_factory=RealDictCursor)) as pg_conn:
        sqlite_cur = sqlite_conn.cursor() 
        load_from_sqlite(sqlite_conn, pg_conn)
