import csv
import re

from pymongo import MongoClient
import pymongo

client = MongoClient()

def read_data(csv_file, db):
    """
    Загрузить данные в бд из CSV-файла
    """
    concert_db = client[db] #База данных
    concert_collection = concert_db['concert_info'] #Коллекция
    with open(csv_file, encoding='utf8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.DictReader(csvfile)
        concerts_list = list(reader)
        for item in concerts_list:
            item['Цена'] = int(item['Цена'])
            concert_collection.insert_one(item)
        print(concert_collection)

def find_cheapest(db):
    """
    Отсортировать билеты из базы по возрастанию цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """
    concert_db = client[db] #База данных
    concert_collection = concert_db['concert_info'] #Коллекция
    list_of_concerts = list(concert_collection.find().sort('Цена'))
    return list_of_concerts

def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке, например "Seconds to"),
    и вернуть их по возрастанию цены
    """
    concert_db = client[db] #База данных
    concert_collection = concert_db['concert_info'] #Коллекция
    regex = re.compile(name)
    result = list(concert_collection.find(
        {'Исполнитель': {'$in': [regex]}}).sort('Цена')
                  )
    return result


if __name__ == '__main__':
    read_data('artists.csv', 'concert_db')
    print(find_cheapest('concert_db'))
    print(find_by_name(' ДжаZ', 'concert_db'))