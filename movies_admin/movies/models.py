"""django модели панели администратора"""

import uuid

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _
from psqlextra.indexes import UniqueIndex


class TimeStampedMixin(models.Model):
    """Класс для представления дат.
    ...
    Атрибуты
    --------
    created_at : datetime
        дата создания
    updated_at : datetime
        дата обновления 
    """
    created_at = models.DateTimeField(auto_now_add=True, verbose_name = _('Created at'))
    updated_at = models.DateTimeField(auto_now=True, verbose_name = _('Updated at'))

    class Meta:        
        abstract = True


class UUIDMixin(models.Model):
    """Класс для хэш-идентификатора
    ...
    Атрибуты
    --------
    id : str
        идентификатор
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False, verbose_name = 'Id')

    class Meta:        
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    """
    Класс для представления жанра. Наследует классы TimeStampedMixin, UUIDMixin.
    ...
    Атрибуты
    --------
    name : str
        название жанра
    description : str
        дата обновления
    """
    name = models.CharField(        
        max_length=255,
        default='', 
        verbose_name = _('Name')
    )
    description = models.TextField(
        verbose_name = _('Description'),
        null=True,
        default=''
    )

    def __str__(self):
        """Выводит название жанра"""
        return str(self.name)

    class Meta:        
        db_table = "content\".\"genre"
        verbose_name = _('Genre')
        verbose_name_plural = _('Genres')


class Person(UUIDMixin, TimeStampedMixin):
    """
    Класс для представления персоны. Наследует классы TimeStampedMixin, UUIDMixin.
    ...
    Атрибуты
    --------
    full_name : str
        полное имя
    """
    full_name = models.CharField(
        max_length=255,        
        default='',
        verbose_name = _('Full Name')       
    )

    def __str__(self):
        """Выводит название полное имя"""
        return str(self.full_name)

    class Meta:        
        db_table = "content\".\"person"
        verbose_name = _('Person')
        verbose_name_plural = _('Persons')


class Filmwork(UUIDMixin, TimeStampedMixin):
    """
    Класс для представления кинопроизведения. Наследует классы TimeStampedMixin, UUIDMixin.
    ...
    Атрибуты
    --------
    title : str
        название
    description : str
    описание
    creation_date : datetime
        дата создания
    file_path : str
        путь до файла
    rating : float
        рейтинг
    type : str
        тип
    """

    class FilmType(models.TextChoices):
        """
        Класс для представления типа кинопроизведения.
        ...
        Атрибуты
        --------
        movie : str
            фильм
        tv_show : str
            тв-шоу
        """
        MOVIE = 'movie', _('Movie')
        TV_SHOW = 'TV_show', _('TV_show')

    title = models.CharField(
        max_length=255,        
        default='', 
        verbose_name = _('Title'),
    )
    description = models.TextField(
        null=True, 
        verbose_name = _('Description')      
    )
    creation_date = models.DateField(         
         verbose_name = _('Creation Date'),
         null=True,
         default=''
    )
    file_path = models.FileField(
        name = 'file_path',
        upload_to='film_works/',
        blank=True, null=True,
        verbose_name = _('FilePath')
    )
    rating = models.FloatField(        
        null=True,        
        validators=[MinValueValidator(0),
                    MaxValueValidator(100)],
        verbose_name = _('Rating')
    )
    type = models.CharField(        
        max_length=25,
        choices=FilmType.choices,        
        default=FilmType.MOVIE,
        verbose_name = _('Type')      
    )
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')
    persones = models.ManyToManyField(Person, through='PersonFilmwork')

    def __str__(self):
        return str(self.title)

    class Meta:        
        db_table = "content\".\"film_work"
        verbose_name = _('Filmwork')
        verbose_name_plural = _('Filmworks')


class GenreFilmwork(UUIDMixin):
    """
    Класс для представления жанра кинопроизведения. Наследует класс UUIDMixin.
    ...
    Атрибуты
    --------
    film_work_id : str
        идентификаторкинопроизведения
    genre_id : str
        идентификатор жанра
    created_at : datetime
        дата создания
    """
    film_work_id = models.ForeignKey(
        'Filmwork',
        on_delete=models.CASCADE,
        db_column='film_work_id',
        verbose_name = _('Filmwork')
    )
    genre_id = models.ForeignKey(
        'Genre',
        on_delete=models.CASCADE,
        db_column='genre_id',
        verbose_name = _('Genre')
    )
    created_at = models.DateTimeField(auto_now_add=True, verbose_name = _('Created at') )

    class Meta:        
        indexes = [
            UniqueIndex(fields=['film_work_id', 'genre_id']),
        ]
        db_table = "content\".\"genre_film_work"
        verbose_name = _('GenreFilmwork')
        verbose_name_plural = _('GenreFilmworks')


class PersonFilmwork(UUIDMixin):
    """
    Класс для представления персоны кинопроизведения. Наследует класс UUIDMixin.
    ...
    Атрибуты
    --------
    film_work_id : str
        идентификатор кинопроизведения
    person_id : str
        идентификатор персоны
    role : str
        роль
    """
    film_work_id = models.ForeignKey(
        'Filmwork',
        on_delete=models.CASCADE,
        db_column='film_work_id',
        verbose_name = _('Filmwork')
    )
    person_id = models.ForeignKey(
        'Person',
        on_delete=models.CASCADE,
        db_column='person_id',
        verbose_name = _('Person')
    )
    role = models.TextField(verbose_name = _('Role'), default='', null = True)
    created_at = models.DateTimeField(auto_now_add=True, verbose_name = _('Created at') )

    class Meta:        
        indexes = [
            UniqueIndex(fields=['film_work_id', 'person_id', 'role']),
        ]
        db_table = "content\".\"person_film_work"
        verbose_name = _('PersonFilmwork')
        verbose_name_plural = _('PersonFilmworks')