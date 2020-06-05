from re import compile, Pattern

from pymongo.collection import Collection
from pymongo.cursor import Cursor


class CustomCollection(Collection):
    def __init__(self, database, name, **kwargs):
        super().__init__(database, name, **kwargs)

    # NOTE: ЧАСТЬ 1
    def get_multiple_files(self, n: int = 20) -> Cursor:
        """1.1.
        Получение первых n файлов"""
        return self.find().limit(n)

    def get_files_name_borough(self, n: int = 20) -> Cursor:
        """1.2.
        Получение n файлов у которых есть поля с названием и районом"""
        return self.find({'borough': {'$exists': True}, 'name': {'$exists': True}}).limit(n)

    def get_files_name_borough_noid(self, n: int = 20) -> Cursor:
        """1.3.
        Получение n файлов у которых есть поля с названием и районом без поля id"""
        return self.find({'borough': {'$exists': True}, 'name': {'$exists': True}}, {'_id': 0}).limit(n)

    def get_fields(self, n: int = 20) -> Cursor:
        """1.4.
        Получение полей restaurant_id, borough, cuisine из n файлов. Исключено поле _id"""
        return self.find(projection={'restaurant_id': 1, 'borough': 1, 'cuisine': 1, '_id': 0}).limit(n)

    def get_restaurant_in_bronx(self, n: int = 20, start: int = 0) -> Cursor:
        """1.5. 1.6. 1.7.
        Получение n файлов о ресторанох в Bronx"""
        return self.find({'borough': 'Bronx'}).skip(start).limit(n)

    def get_restaurant_top(self, n: int = 20, min_rating: int = None, max_rating: int = None) -> Cursor:
        """1.8. 1.9.
        Получение n ресторанов с рейтингом между min_rating и max_rating"""
        res: Cursor
        if min_rating and max_rating:
            res = self.find({'grades': {'$elemMatch': {'score': {'$gt': min_rating, '$lt': max_rating}}}}).limit(n)
        elif min_rating:
            res = self.find({'grades': {'$elemMatch': {'score': {'$gt': min_rating}}}}).limit(n)
        elif max_rating:
            res = self.find({'grades': {'$elemMatch': {'score': {'$lt': max_rating}}}}).limit(n)
        return res

    def get_restaurant_south(self, n: int = 20, latitude: int = -95, cuisine: str = None) -> Cursor:
        """1.10. 1.11.
        Получение n ресторанов южнее latitude и не готовящие cuisine"""
        res: Cursor
        if cuisine:
            res = self.find({'address.coord.0': {'$lt': latitude}, 'cuisine': {'$ne': cuisine}}).limit(n)
        else:
            res = self.find({'address.coord.0': {'$lt': latitude}}).limit(n)
        return res

    # NOTE: ЧАСТЬ 2
    def run_query(self, n: int = 20) -> Cursor:
        """2.1.
        Использование оператора $query"""
        return self.find({'$query': {'address.coord.0': {'$lt': -65}, 'grades.score': {'$gt': 70},
                                     'cuisine': {'$ne': 'American '}}}).limit(n)

    def analysis_query(self) -> Cursor:
        """2.2.
        Данный запрос возвращает набор ресторанов,котрые
         не готовят Американскую кухню,
         хотя бы раз получали класс А,
         а так же не находятся в Бруклине.
        Вся выборка сортеруется по "кухне" Z-A
        """
        res = self.find(
            {'$query':
                {
                    'cuisine': {'$ne': 'American '},
                    'grades.grade': 'A',
                    'borough': {'$ne': 'Brooklyn'}
                },
                '$orderby': {'cuisine': -1}
            }
        )
        return res

    def regx_query(self, n: int = 20, regx: str = r'^Wil.*') -> Cursor:
        """2.3. 2.4. 2.5.
        Использование регулярных выражений. Получение ресторанов в названии которых есть regx"""
        regx: Pattern = compile(regx)
        return self.find({'name': regx}, {'restaurant_id': 1, 'name': 1, 'borough': 1, 'cuisine': 1}).limit(n)

    def get_restaurant_in_bronx_americanorchinese(self, n: int = 20) -> Cursor:
        """2.6.
        Все рестораны в Бронксе готовящие Американскую или Китайскую кухни"""
        return self.find({'borough': 'Bronx', '$or': [{'cuisine': 'American '}, {'cuisine': 'Chinese'}]}).limit(n)

    def get_restaurant_in_several_borough(self, n: int = 20) -> Cursor:
        """2.7.
        Выбор всех ресторанов из Стайтен-Айленда, Куинса, Бруклина и Бронкса"""
        return self.find({'$or': [{'borough': 'Staten Island'}, {'borough': 'Queens'}, {'borough': 'Brooklyn'},
                                  {'borough': 'Bronx'}]}).limit(n)

    def get_restaurant_less_10(self, n: int = 20) -> Cursor:
        """2.8.
        Рестораны ниже 10 балов"""
        return self.find({'grades.score': {'$lt': 10}},
                         {'restaurant_id': 1, 'name': 1, 'borough': 1, 'cuisine': 1}).limit(n)

    def lot_of_filters(self, n: int = 20) -> Cursor:
        """2.9.
        Рестораны не готовящие китайскую и американскую кухню или начинающиеся на Wil"""
        regx: Pattern = compile(r'^Wil.*')
        return self.find({'$or': [{'$and': [{'cuisine': {'$ne': 'American '}}, {'cuisine': {'$ne': 'Chinese'}}]},
                                  {'name': {'$regex': regx}}], },
                         {'restaurant_id': 1, 'name': 1, 'borough': 1, 'cuisine': 1}).limit(n)

    def get_restaurant_find_longitude(self, n: int = 20, min_longitude: int = 42, max_longitude: int = 52) -> Cursor:
        """2.10.
        Рестораны находяшиеся между min_longitude и max_longitude"""
        return self.find({'address.coord.1': {'$gt': min_longitude, '$lt': max_longitude}},
                         {'restaurant_id': 1, 'name': 1, 'borough': 1, 'cuisine': 1}).limit(n)

    # NOTE: ЧАСТЬ 3
    def sort_by_name(self, n: int = 20, revers: bool = False) -> Cursor:
        """"3.1. 3.2
        Сортировка всех ресторанов по названию (по возрастанию)"""
        res: Cursor
        if revers:
            res = self.find().sort('name', -1).limit(n)
        else:
            res = self.find().sort('name').limit(n)
        return res

    def count_restaurant_have_street(self) -> int:
        """3.3.
        Подсчет количества ристронов у которых есть информация об улицей"""
        return self.find({'address.street': {'$exists': True}}).count()

    def get_restaurant_grades_7(self, n: int = 20) -> Cursor:
        """3.4.
        Рестораны с оценками кратными 7"""
        return self.find({'grades': {'$elemMatch': {'score': {'$mod': [7, 0]}}}},
                         {'restaurant_id': 1, 'name': 1, 'borough': 1, 'cuisine': 1}).limit(n)

    def get_restaurant_regx_mon(self, n: int = 20) -> Cursor:
        """3.5.
        Рестораны содержащие в name, borough, coord или cuisine mon"""
        regx: Pattern = compile(r'.*mon.*')
        return self.find({'$or': [{'name': {'$regex': regx}}, {'borough': {'$regex': regx}},
                                  {'coord': {'$regex': regx}}, {'cuisine': {'$regex': regx}}]},
                         {'restaurant_id': 1, 'name': 1, 'borough': 1, 'cuisine': 1}).limit(n)
