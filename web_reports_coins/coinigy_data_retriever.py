from __future__ import print_function

import pymongo
import threading
import datetime
from ast import literal_eval
import redis

from lxml import html
import os
import sys
import datetime as dt
import json
import requests
from time import sleep
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

# from common import get_arg
# Update Python Path to be able to load custom modules. Do not change line position.


class Subscriber:
    def __init__(self, name=None):
        if not name:
            self.name = str(self.__class__).split(' ')[1].split("'")[1]
        else:
            self.name = name

    def update(self, message):
        # start new Thread in here to handle any task
        print('\n\n {} got message "{}"'.format(self.name, message))


class MyMongoClient(Subscriber):

    def __init__(self, db_name, collection_name, host='localhost', is_exchange_data=False, port=27017, *args, **kwargs):
        self._c = pymongo.MongoClient(host, port)
        self.set_database(db_name)
        self.set_collection(collection_name)
        self.collection_name = collection_name
        self.db_name = db_name
        self.redis = redis.Redis(host='localhost', port=6379, db=0)
        self.is_exchange_data = is_exchange_data

    def is_duplicate_data(self, msg):
        return self.collection.find_one(msg)

    def insert_one(self, data):
        self.collection.insert_one(data)
        print('------table name-----')
        print(self.collection)
        print('Inserted: \n{}'.format(data))

    def insert_many(self, msg):
        self.collection.insert_many(msg)

    def update(self, msg):
        # msg = literal_eval(msg)
        if not self.is_exchange_data:
            t = threading.Thread(target=self.check_duplicity_and_update_record, args=(msg,))
        else:
            t = threading.Thread(target=self.set_in_redis, args=(msg,))
        t.start()

    def check_duplicity_and_update_record(self, msg):
        if not self.is_duplicate_data(msg):
            t = threading.Thread(target=self.insert_many, args=(msg,)) if type(msg) == list else threading.Thread(
                target=self.insert_one, args=(msg,))
            t.start()

    def set_in_redis(self, msg):
        key = datetime.datetime.now().replace(second=0, microsecond=0)
        data = {self.db_name: {self.collection_name: [msg]}}

        if (self.redis.exists(key)):
            data = literal_eval(self.redis.get(key))
            print(data)
            if (data.has_key(self.db_name) and data[self.db_name].has_key(self.collection_name)):

                if type(msg) == list:
                    data[self.db_name][self.collection_name].extend(msg)
                else:
                    data[self.db_name][self.collection_name].append(msg)
            elif data.has_key(self.db_name):
                data[self.db_name].update({self.collection_name: [msg]})
        self.redis.set(key, data)

    def set_collection(self, collection_name):
        self.collection = self.database[collection_name]

    def set_database(self, db_name):
        self.database = self._c[db_name]


class TradingMongoClient(MyMongoClient):
    def insert_one(self, data):
        """
        Parses the message and converts it into a dictionary before storing it
        """
        data = {data[0].isoformat().split(".")[0]: data}
        self.collection.insert_one(data)
        print('Inserted: \n{}'.format(data))


def get_arg(index, default=None):
    """
    Grabs a value from the command line or returns the default one.
    """
    try:
        return sys.argv[index]
    except IndexError:
        return default


def get_data():
    data = requests.get('https://coinmarketcap.com/all/views/all/')
    data_content = html.fromstring(data.content)
    db_name = 'cc_coins'

    currency_lists = data_content.xpath(
        '//div[@class="table-responsive"]//tbody/tr')
    # currency_names = data_content.xpath(
    #     '//div[@class="table-responsive"]//tbody//td[@class="no-wrap currency-name"]//a//text()')

    for currency_list in currency_lists:
        # currency_name = str(currency_list.xpath('./td[@class="no-wrap currency-name"]//a//text()')[0])
        currency_data = [i.strip() for i in currency_list.xpath(
            './td//text()') if i.strip()]

        data = {
            'timestamp': dt.datetime.now(),
            'Symbol': currency_data[2],
            'Market Cap': currency_data[3],
            'Price': currency_data[4],
            'Circulating Supply': currency_data[5],
            'Volume (24h)': currency_data[6],
            '% 1h': currency_data[7],
            '% 24h': currency_data[8],
            '% 7d': currency_data[9]
        }
        currency_name = currency_data[2]
        coin_name = get_arg(1, currency_name)
        # key = 'b7c761ab7ca0fbe560a1c6941f49e8b0'
        # secret = '72fc68c63ba7ab80cc65fb9cf1d214f7'

        collection = '{}'.format(coin_name)
        try:
            db_user = 'Writeuser'
            db_password = os.environ['MONGO-WRITE-PASSWORD']
            host = 'mongodb://{}:{}@127.0.0.1'.format(db_user, db_password)
        except KeyError:
            host = 'localhost'

        # coin = CoinigyCoin(key, secret)

        db = MyMongoClient(db_name, collection_name=collection,
                           host=host)
        db.insert_one(data)

if __name__ == "__main__":

        # Time setting.
        next_call = dt.datetime.now()
        time_between_calls = dt.timedelta(seconds=int(get_arg(2, 30)))
        # Main loop.
        while True:
            now = dt.datetime.now()
            if now >= next_call:
                try:
                    get_data()
                    next_call = now + time_between_calls
                except:
                    sleep(600)
                    continue
            else:
                sleep(600)
