"""панель администратора"""

from django.contrib import admin
from django.utils.translation import gettext_lazy as _

from .models import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    """
    Класс для представления жанра в админке.
    ...
    Атрибуты
    --------
    list_display : tuple
        поля для отображения
    """
    list_display = ('name', 'description', 'updated_at')


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    """
    Класс для представления персоны в админке.
    ...
    Атрибуты
    --------
    list_display : tuple
        поля для отображения
    """
    list_display = ('full_name', 'updated_at')


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    """
    Класс для представления кинопроизведения с жанром и персонами в админке.
    ...
    Атрибуты
    --------
    inlines : tuple
        передающиеся классы
    list_display : tuple
        поля для отображения
    list_filter : tuple
        поля для фильтрации
    search_fields : tuple
        поля для поиска
    """
    inlines = (GenreFilmworkInline, PersonFilmworkInline,)
    # Отображение полей в списке
    list_display = ('title', 'description', 'type', 'rating', 'creation_date', 'created_at', 'updated_at')
    # Фильтрация в списке
    list_filter = ('type',)
    # Поиск по полям
    search_fields = ('title', 'description', 'id')
