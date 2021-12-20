import requests
import json
import psycopg2


def main():
    url = 'https://api.coincap.io/v2/rates/bitcoin'
    r = requests.get(url)
    r.encoding = 'utf-8'
    data = json.loads(r.text)

    id = data['data']['id']
    symbol = data['data']['symbol']
    currencysymbol = data['data']['currencySymbol']
    rateUsd = data['data']['rateUsd']
    type =  data['data']['type']

    insert_bitcoinRates(id, symbol, currencysymbol, rateUsd, type)



def insert_bitcoinRates(id, symbol, currencysymbol, rateusd, type):
    try:
        conn = psycopg2.connect(host="localhost", database="analytics", user="postgres", password="3321", port="5432")
        # conn = psycopg2.connect("host=localhost dbname=analytics user=postgres password=3321")
        cur = conn.cursor()
        #cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
        cur.execute("insert into bitcoinrates (id, symbol, currencysymbol, rateusd, type) VALUES (%s, %s, %s, %s, %s)", (id, symbol, currencysymbol, rateusd, type))

        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)   
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    main()
