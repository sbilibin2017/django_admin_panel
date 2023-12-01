"""тесты для панели администратора"""

import os
import pathlib
import sys
from contextlib import closing

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

sys.path.insert(
    0,
    os.path.join(pathlib.Path(__file__).parent.absolute(), '../..')
)
from db_settings import DSL
from src import conn_context

load_dotenv()

db = os.environ.get('DB_SQLITE_NAME')
CHUNK_SIZE = int(os.environ.get('CHUNK_SIZE'))

with conn_context(db) as sqlite_conn, \
        closing(psycopg2.connect(**DSL, cursor_factory=RealDictCursor)) as pg_conn:
    sqlite_cur = sqlite_conn.cursor()
    for table_name in (
        'person', 'genre', 'film_work',
        'person_film_work', 'genre_film_work'
    ):
        person_count_v1 = sqlite_cur.execute( \
            f"""SELECT count(*) as  count FROM {table_name}"""
        )
        x1 = sqlite_cur.fetchone()
        with pg_conn.cursor() as pg_cur:
            person_count_v2 = pg_cur.execute( \
                f"""SELECT count(*) as count FROM content.{table_name}"""
            )
            x2 = pg_cur.fetchone()
        n1 = list(x1.values())[0]
        n2 = list(x2.values())[0]
        assert n1 == n2, f'{table_name} - размеры не совпадают'

        person_count_v1 = sqlite_cur.execute( \
            f"""SELECT * FROM {table_name}""")
        while True:
            x1 = sqlite_cur.fetchmany(CHUNK_SIZE)
            if x1:
                idxs = tuple([str(el['id']) for el in x1])
                with pg_conn.cursor() as pg_cur:
                    pg_cur.execute(
                        f"""SELECT * FROM content.{table_name} \
                            WHERE content.{table_name}.id in {idxs};"""
                    )
                    x2 = pg_cur.fetchmany(CHUNK_SIZE)
                assert len(x1) == len(x2)
                for i in range(len(x1)):
                    assert x1[i] == x1[i], f'{table_name} - значения не совпадают'
            else:
                break
