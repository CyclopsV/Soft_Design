from pymongo import MongoClient

from collection import CustomCollection
from config import Config

client: MongoClient = MongoClient(Config.ip, int(Config.port))
print(client)
collection: CustomCollection = CustomCollection(client.restaurants, 'rests')

_1_1_first_20 = collection.get_multiple_files()
_1_2_name_and_borough = collection.get_files_name_borough()
_1_3_no_id = collection.get_files_name_borough_noid()
_1_4_restaurantid_name_borough_cuisine_noid = collection.get_fields()

_1_5_restaurant_in_bronx = collection.get_restaurant_in_bronx(n=0)
_1_6_restaurant_5_in_bronx = collection.get_restaurant_in_bronx(n=5)
_1_7_restaurant_10_20_in_bronx = collection.get_restaurant_in_bronx(n=10, start=10)

_1_8_restaurant_top_90 = collection.get_restaurant_top(min_rating=90)
_1_9_restaurant_top_90_109 = collection.get_restaurant_top(min_rating=90, max_rating=109)

_1_10_restaurant_south_95 = collection.get_restaurant_south()
_1_11_restaurant_south_65_noamerican = collection.get_restaurant_south(latitude=-65, cuisine='American ')

_2_1_query = collection.run_query()
_2_2_analysis = collection.analysis_query()

_2_3_regx_start = collection.regx_query()
_2_4_regx_end = collection.regx_query(regx=r'.*ces$')
_2_5_regx_include = collection.regx_query(regx=r'.*Reg.*')

_2_6_in_bronx_american_and_chinese = collection.get_restaurant_in_bronx_americanorchinese()
_2_7_restaurant_in_several_borough = collection.get_restaurant_in_several_borough()
_2_8_restaurant_less_10 = collection.get_restaurant_less_10()
_2_9_no_ac_or_wil = collection.lot_of_filters()
_2_10_restaurant_find_longitude = collection.get_restaurant_find_longitude()

_3_1_sort_by_name = collection.sort_by_name()
_3_2_sort_by_name_r = collection.sort_by_name(revers=True)
_3_3_count_restaurant = collection.count_restaurant_have_street()
_3_4_restaurant_grades_7 = collection.get_restaurant_grades_7()
_3_5_restaurant_regx_mon = collection.get_restaurant_regx_mon()
