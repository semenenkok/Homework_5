import requests
import json
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors
from config import config


def main():
    url = 'https://api.coincap.io/v2/rates/bitcoin'
    r = requests.get(url)
    r.encoding = 'utf-8'
    if r.status_code == 200:
        data = json.loads(r.text)

        id = data['data']['id']
        symbol = data['data']['symbol']
        currencysymbol = data['data']['currencySymbol']
        rateUsd = data['data']['rateUsd']
        type =  data['data']['type']

        insert_bitcoinRates(id, symbol, currencysymbol, rateUsd, type)
    else:
        print('api response code is: ' + str(r.status_code))
        raise

def insert_bitcoinRates(id, symbol, currencysymbol, rateusd, type):
    conn = None
    try:
        conn = psycopg2.connect(host="localhost", database="analytics", user="postgres", password="3321", port="5432")
    except (Exception, psycopg2.DatabaseError) as err:
        print("Database connection error: {0}".format(err))
        raise

    if conn is not None:
        try:
            cur = conn.cursor()
            #cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
            cur.execute("insert into bitcoinrates (id, symbol, currencysymbol, rateusd, type) VALUES (%s, %s, %s, %s, %s)", (id, symbol, currencysymbol, rateusd, type))
            conn.commit()
            cur.close()
        except (Exception) as error:
            print(error)   
            raise
        finally:
            conn.close()


if __name__ == '__main__':
    main()
