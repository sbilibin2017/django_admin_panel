"""проверка идентичности sqlite3 м postgre БД"""

import logging
import os
import sqlite3
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Dict, Generator

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()

DEFAULT_VALUE = ''

@dataclass
class TimeStampedMixin():
    """Класс для представления дат."""
    created_at: datetime = field(default= datetime.now())
    updated_at: datetime = field(default= datetime.now())

    class Meta:
        abstract = True


@dataclass
class UUIDMixin():
    """Класс для хэш-идентификатора"""
    id: uuid.UUID = field(default_factory=uuid.uuid4)

    class Meta:        
        abstract = True


@dataclass
class Person(UUIDMixin, TimeStampedMixin):
    """Класс для представления персоны."""    
    full_name: str = field(default=DEFAULT_VALUE)


@dataclass
class Genre(UUIDMixin, TimeStampedMixin):
    """Класс для представления жанра."""    
    name: str = field(default=DEFAULT_VALUE)
    description: str = field(default=None)
    

@dataclass
class Filmwork(UUIDMixin, TimeStampedMixin):
    """Класс для представления кинопроизведения."""
    title: str = field(default=DEFAULT_VALUE)
    description: str = field(default=None)
    creation_date: datetime = field(default=None)
    file_path: str = field(default=None)
    rating: float = field(default=None)
    type: str = field(default=('Movie'))


@dataclass
class GenreFilmwork(UUIDMixin):
    """Класс для представления жанра кинопроизведения."""
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)
    genre_id: uuid.UUID = field(default_factory=uuid.uuid4)
    created_at: datetime = field(default=datetime.now())


@dataclass
class PersonFilmwork(UUIDMixin):
    """Класс для представления персоны кинопроизведения."""    
    role: str = field(default='')
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)
    person_id: uuid.UUID = field(default_factory=uuid.uuid4)
    created_at: datetime = field(default=datetime.now())
    

class SQLiteExtractor:
    """Класс для извлечения данных из sqlite3 БД."""
    def __init__(self, conn: sqlite3.Connection):
        self.sql_conn = conn.cursor()
        self.table_name = (
            'person',
            'genre',
            'film_work',
            'genre_film_work',
            'person_film_work'
        )

    def extract_movies(self):
        """Извлекает данные из таблицы"""
        for table_name in self.table_name:
            try:
                self.sql_conn.execute(
                    f"""SELECT * FROM {table_name}"""
                )
                logging.info('sqlite3 connected')
            except sqlite3.Error as e:
                logging.exception(e)
                break
            
            while True:
                rows = self.sql_conn.fetchmany(int(os.environ.get('CHUNK_SIZE')))
                if rows:
                    yield (table_name, rows) 
                else:
                    break


class PostgresSaver:
    """Класс для загрузки данных в postre БД."""
    def __init__(self, psql_conn: RealDictCursor) -> None:
        self.psql_conn = psql_conn
        self.table_name = (
            'person',
            'genre',
            'film_work',
            'genre_film_work',
            'person_film_work')
        self.default_value = DEFAULT_VALUE

    def insert_query(
            self,
            table_name: str,
            row: Dict) -> None:
        """Вставляет данные в таблицу"""
        cols = ','.join(row.keys())
        qmarks = ','.join(['%s' for s in row.keys()])
        values = tuple(row.values())
        insert_statement = f"INSERT INTO content.{table_name} ({cols}) VALUES ({qmarks}) ON CONFLICT DO NOTHING;"
        
        with self.psql_conn.cursor() as cur:
            try:
                cur.execute(insert_statement, values)
                self.psql_conn.commit()
                logging.info('postrgresql commit success')
                logging.info(f'query: {insert_statement}. values: {values}')
            except psycopg2.Error as e:
                logging.exception(e)    

    def validate_person(self, row):
        """Валидация персоны"""
        created = Person(
            id=row['id'],
            full_name=self.default_value if row['full_name'] is None \
                else row['full_name'],
            created_at=datetime.now() if row['created_at'] is None else row['created_at'],
            updated_at=datetime.now() if row['updated_at'] is None else row['updated_at']
        )
        
        return created   

    def validate_genre(self, row):
        """Валидация жанра"""
        created = Genre(
            id=row['id'],
            name=self.default_value if row['name'] is None else row['name'],
            description=row['description'],
            created_at=datetime.now() if row['created_at'] is None else row['created_at'],
            updated_at=datetime.now() if row['updated_at'] is None else row['updated_at']
        )
        
        return created       

    def validate_film_work(self, row):
        """Валидация кинопроизведения"""
        created = Filmwork(
            id=row['id'],
            title=self.default_value if row['title'] is None else row['title'],
            description=row['description'],
            creation_date=row['creation_date'],
            file_path=row['file_path'],
            rating=row['rating'],
            type='movie' if row['type'] is None else row['type'],
            created_at=datetime.now() if row['created_at'] is None else row['created_at'],
            updated_at=datetime.now() if row['updated_at'] is None else row['updated_at']
        )
        
        return created     

    def validate_genre_film_work(self, row):
        """Валидация жанра кинопроизведения"""
        created = GenreFilmwork(
            id=row['id'],
            film_work_id=row['film_work_id'],
            genre_id=row['genre_id'],
            created_at=datetime.now() if row['created_at'] is None else row['created_at'],
        )
        
        return created   

    def validate_person_film_work(self, row):
        """Валидация персоны кинопроизведения"""
        created = PersonFilmwork(
            id=row['id'],
            role=row['role'],
            film_work_id=row['film_work_id'],
            person_id=row['person_id'],
            created_at=datetime.now() if row['created_at'] is None else row['created_at'],            
        )   

        return created

    def save_all_data(self, gen: Generator) -> None:
        """Валидирует данные и вставляет их в БД"""
        for tup in gen:
            table_name, rows = tup
            for row in rows:                
                if table_name == 'person':
                    created = self.validate_person(row) 
                elif table_name == 'genre':
                    created = self.validate_genre(row)                            
                elif table_name == 'film_work':
                    created = self.validate_film_work(row) 
                elif table_name == 'person_film_work':
                    created = self.validate_person_film_work(row)                    
                elif table_name == 'genre_film_work':
                    created = self.validate_genre_film_work(row)
                row_validated = asdict(created)   
                self.insert_query(
                    table_name=table_name,
                    row=row_validated
                )
                                  
                
                
            