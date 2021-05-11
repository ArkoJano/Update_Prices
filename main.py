import requests
from requests.exceptions import HTTPError
import mysql.connector
from mysql.connector import errorcode
from decimal import Decimal
import pprint


class DBConnector:
    def __init__(self,user_name, passwd, host, dbname):
        self.user_name = user_name
        self.passwd     = passwd
        self.host      = host
        self.dbname    = dbname


    def connect_to_db(self):

        try:
            self.db = mysql.connector.connect(
                host=self.host,
                user=self.user_name,
                passwd=self.passwd,
                database=self.dbname,
            )
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        
    def close_connection(self):
        self.db.close()

    def get_all_products(self):
        UnitPrice = 6
        cursor = self.db.cursor()
        
        cursor.execute("SELECT * FROM product")
        self.records = cursor.fetchall()
        

        cursor.close()


    def update_USD_price(self, currency):

        UnitPrice = 6
        cursor = self.db.cursor()
    
        self.get_all_products()

        for row in self.records:
            query = f"""UPDATE product SET UnitPriceUSD = {Decimal(currency)*row[UnitPrice]}
                        WHERE UnitPrice = {row[UnitPrice]}"""
            cursor.execute(query)

        self.db.commit()

        cursor.close()

    def update_Euro_price(self, currency):
        UnitPrice = 6

        cursor = self.db.cursor()
        
        self.get_all_products()

        for row in self.records:
            
            query = f"""UPDATE product SET UnitPriceEuro = {Decimal(currency)*row[UnitPrice]}
                        WHERE UnitPrice = {row[UnitPrice]}"""
            cursor.execute(query)

        self.db.commit()

        cursor.close()

class NBPConnector():

    def __init__(self):
        self.api_base_url = 'http://api.nbp.pl/api/'
        self.tableA = 'a/'
        self.tableB = 'b/'
        self.tableC = 'c/'
        self.exchange_rates = 'exchangerates/rates/'
        self.today = 'today'

   

    def get_updated_currency_USD(self):

        usd = 'usd/'
        endpoint_today = f"{self.api_base_url}{self.exchange_rates}{self.tableA}{usd}"
        endpoint = f"{self.api_base_url}{self.exchange_rates}{self.tableA}{usd}"

        r = requests.get(endpoint)
        data = r.json()

        usd_currency = data['rates'][0]['mid']
        return usd_currency
        

    def get_updated_currency_Euro(self):
        euro = 'eur/'
        endpoint_today = f"{self.api_base_url}{self.exchange_rates}{self.tableA}{euro}{self.today}"
        endpoint = f"{self.api_base_url}{self.exchange_rates}{self.tableA}{euro}"
        try:
            r = requests.get(endpoint_today)
            r.raise_for_status()
        except HTTPError as err:
            status_code = err.response.status_code

        if status_code == 404:
            r = requests.get(endpoint)

        data = r.json()

        euro_currency = data['rates'][0]['mid']
        return euro_currency
        

class make_excel_file():
    pass

db = DBConnector('root', '1234', 'localhost', 'mydb')

db.connect_to_db()

#db.update_USD_price(2)


nbp = NBPConnector()
usd_currency = nbp.get_updated_currency_USD()
euro_currency = nbp.get_updated_currency_Euro()

db.update_Euro_price(euro_currency)
db.update_USD_price(usd_currency)

db.close_connection()
