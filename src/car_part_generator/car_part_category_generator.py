import pandas as pd
from collections import Counter
from xlwt import Workbook
import re
import json

with open("config.json") as json_data_file:
    print('Reading config')
    config_data = json.load(json_data_file)
    print('Config loaded')

# name of output excel
output_excel = config_data['output_excel_path']

print('Reading excel files')
# read excels and rename columns
df_car_parts = pd.read_excel(r'' + config_data['car_parts_excel_path'])
df_car_parts.columns = ['id', 'product_code', 'product_name', 'second_name_of_the_products', 
                        'description_of_the_products', 'common_price', 'price', 'image_(original_size)', 
                        'image_(prefix)', 'file_to_be_uploaded', 'name_of_the_file', 'unit', '', 'parameter_1',
                        'parameter_2', 'parameter_3', 'image_1', 'image_2', 'image_3', 'image_4', 'image_5',
                        'image_6_related_products_(separated_by__|_',
                        '_maximum_8_products)_;_added_property_lists_(separated_by__|_)_', 'new_(1_|_0)',
                        'in_stock_(1_|_0)', 'number_of_stock', 'availability_(availability_id)',
                        'appeared_on_home_page_(1_|_0)', 'most_sold_(1_|_0)', 'special_offer_(1_|_0)',
                        'authorization_of_contributions_(1_|_0)', 'used_(1_|_0)', 'tax_(1_|_2_|_3)', 'manufacturer',
                        'barcode', 'shipping_fee_supplements_(x_ao)', 'weight', 'file_to_upload_(2)', 'file_name_(2)',
                        'do_not_show_the_products_in_the_e-shop', 'hide_button', 'shipping', 'coupon_cannot_be_redeemed',
                        'keywords_separated_by_commas', 'short_description_of_the_page_', 'page_title',
                        'xml_feed_does_not_contain_products', 'variants', 'url_cm', 'base', 'parameters',
                        'my_parameter_order', 'minimum_can_you_order', '']

df_cars = pd.read_excel(r'' + config_data['cars_excel_path'])
df_cars.rename(columns={'id': 'sub_id'}, inplace=True)
df_cars.rename(columns={'id_zakaznik': 'subcategory_id'}, inplace=True)
df_cars.rename(columns={'id_kategoriesk': 'id'}, inplace=True)
df_cars.rename(columns={'kategorie2': 'category_name'}, inplace=True)

df_cars_and_parts = pd.read_excel(r'' + config_data['cars_and_parts_excel_path'])
df_cars_and_parts.columns = ['id', 'reference', 'application', 'test']
print('Excel files loaded')

# get article number which is in the car parts and cars and parts excel
def get_common_article_no(cars_and_parts_ids, cars_parts_article_no_list):
    common_article_no_list = list()
    for cars_and_parts_id in cars_and_parts_ids:
        if isinstance(cars_and_parts_id, str) and len(cars_and_parts_id) > 80:
            article_no = cars_and_parts_id.split(' ')[0].strip()
            if article_no in cars_parts_article_no_list:
                common_article_no_list.append(article_no)

    return common_article_no_list

# get dictionary based on article number index - article_no + cars
def get_common_article_no_with_cars(common_article_no_list):
    common_article_no_with_cars_dict = dict()
    for common_article_no in common_article_no_list:
        index = df_cars_and_parts[df_cars_and_parts['id'].str.contains(common_article_no, na=False)].index[0]
        common_article_no_with_cars_dict[common_article_no] = df_cars_and_parts.loc[index + 1]['application']

    return common_article_no_with_cars_dict

# get car names
def get_car_names(category_name_list):
    first_part_name_list = list()
    for category_name in category_name_list:
        if type(category_name) == str:
            first_part_category_name = category_name.split(' ')[0]
            if len(first_part_category_name) > 3:
                first_part_name_list.append(first_part_category_name)

    return first_part_name_list

# distinct car types
def get_distinct_car_types(first_part_name_list):
    first_part_name_list_counter = Counter(first_part_name_list)
    car_types_set = set()
    for key, value in first_part_name_list_counter.items():
        if value > 1 and value < 20 and key.isalpha():
            car_types_set.add(key.upper())

    return car_types_set

# get cars and its dates
def get_cars(category_name_list, car_types_set):
    specialized_car_types_list = list()
    for elem in category_name_list:
        if isinstance(elem, str):
            splited_elem = elem.split(' ')
            if len(splited_elem) > 3:
                car_name = splited_elem[0].upper()
                car_type = splited_elem[1]
                if car_name in car_types_set and len(car_type) <= 3:
                    specialized_car_types_list.append(car_name + ' ' + car_type)

    return specialized_car_types_list;

# regex to find cars
def regex_find_cars(common_article_no_with_cars_dict, specialized_car_types_list):
    found_article_no_car_combination_dict = dict()
    for article_no, cars in common_article_no_with_cars_dict.items():
        for car_type in specialized_car_types_list:
            regex = car_type + ' '
            pattern_car_and_date = re.compile(regex)
            car_date_list = pattern_car_and_date.findall(cars)
            car_date_list = [s.strip() for s in car_date_list]
            if car_type in car_date_list:
                index_of_car = car_date_list.index(car_type)
                car_name = car_date_list[index_of_car]
                if article_no in found_article_no_car_combination_dict:
                    found_article_no_car_combination_dict[article_no].append(car_name)
                else:
                    found_article_no_car_combination_dict[article_no] = [car_name]

    return found_article_no_car_combination_dict;

def get_index_by_car_name(car_name_input):
    df_cars_dict = df_cars.T.to_dict('list')
    for index, list in df_cars_dict.items():
        car_name = list[3]
        len_of_car = len(car_name_input)
        if car_name and isinstance(car_name, str) and car_name[0:len_of_car] == car_name_input:
            return index

# get final value based on car name
def get_final_list_from_index(index):
    final_list = []
    car = df_cars.loc[index]
    car_id = car['id']
    car_sub_id = car['sub_id']
    car_motor_id_list = df_cars.loc[df_cars['id'] == car_sub_id]['sub_id'].values
    for car_motor_id in car_motor_id_list:
        car_motor_sub_category_id_list = df_cars.loc[df_cars['id'] == car_motor_id]['sub_id'].values
        for car_motor_sub_category_id in car_motor_sub_category_id_list:
            final_list.append(str(car_id) + '-' + str(car_sub_id) + '-' + str(car_motor_id) + '-' + str(car_motor_sub_category_id))

    return final_list

def load_to_excel(list):
    if list:
        wb = Workbook()

        # add_sheet is used to create sheet.
        sheet = wb.add_sheet('article_no_and_code')
        sheet.write(0, 0, 'article_no')
        sheet.write(0, 1, 'code')

        index = 1
        for elem in list:
            parts = elem.split('#')
            key = parts[0]
            value = parts[1]
            sheet.write(index, 0, key)
            sheet.write(index, 1, value)
            index += 1

        wb.save(output_excel)
        print('Data is loaded into Excel named: ' + output_excel)
    else:
        print('Empty data. Nothing will be loaded into excel')

if __name__ == '__main__':
    try:
        print('App started')
        cars_parts_article_no_list = df_car_parts['second_name_of_the_products'].values
        cars_and_parts_ids = df_cars_and_parts['id'].values
        application_column = df_cars_and_parts['application']
        category_name_list = df_cars['category_name'].values

        common_article_no_list = get_common_article_no(cars_and_parts_ids, cars_parts_article_no_list)
        common_article_no_with_cars_dict = get_common_article_no_with_cars(common_article_no_list)
        first_part_name_list = get_car_names(category_name_list)
        car_types_set = get_distinct_car_types(first_part_name_list)
        specialized_car_types_list = get_cars(category_name_list, car_types_set)
        found_article_no_car_combination_dict = regex_find_cars(common_article_no_with_cars_dict, specialized_car_types_list)

        df_cars['category_name'] = df_cars['category_name'].str.upper()

        final_list = list()
        for article_no, car_list in found_article_no_car_combination_dict.items():
            for car in car_list:
                index = get_index_by_car_name(car)
                list = get_final_list_from_index(index)
                code = str(list).replace('[', '').replace(']', '').replace('\'', '').replace(', ', '|').strip()
                final_list.append(article_no + '#' + code)
                print('Data is loading...')

        load_to_excel(final_list)
        print('App finished')
    except Exception as e:
        print("Failed to finish app ... ")
        print(repr(e))
        raise e