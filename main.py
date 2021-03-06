import requests
from requests.exceptions import HTTPError
import mysql.connector
from decimal import Decimal
from pprint import pprint
import logging
import pandas as pd



USER_NAME   = 'root'
PASSWORD    = '1234'
HOST        = 'localhost'
DATABASE    = 'mydb'

EXCEL_FILE  = 'product.xlsx'
LOG_FILE    = "log.log"

# basicConfig calego moduli generujacego logi
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, datefmt='%d-%m-%y %H:%M:%S',
        format="%(asctime)s: %(levelname)s: %(message)s"
        )

class DBConnector:

    '''
    Klasa odpowiedzialna za caly proces polaczenia
    sie z baza danych, pobraniu z niej wartosci i 
    ich aktualizacji.
    '''

    def __init__(self,user_name, passwd, host, dbname):
        self.user_name = user_name
        self.passwd    = passwd
        self.host      = host
        self.dbname    = dbname

        logging.info(f"Created new DBConnector")


    def connect_to_db(self):
        '''
        Metoda odpowiada za podlaczenie sie z odpowiednia
        baza danych. Jesli sie to nie powiedzie to skrypt
        zakonczy swoje dzialanie i zapisze odpowiedni
        log error do pliku log.log.
        '''

        try:
            self.db = mysql.connector.connect(
                host=self.host,
                user=self.user_name,
                passwd=self.passwd,
                database=self.dbname,
            )
            if self.db.is_connected() == True:
                logging.info(f"Connected to {self.host}:{self.dbname}, as {self.user_name}")
        except mysql.connector.Error as err:
            if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
                logging.error("Connection error: Wrong user name or password")
            elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
                logging.error("Connection error: Database does not exist")
            else:
                logging.error(f"Connection error:{err}")
            exit()
        
    def close_connection(self):
        self.db.close()
        logging.info(f"Closed connnection with {self.host}:{self.dbname}")


    def update_USD_price(self, currency):

        '''
        Metoda przyjmuje jako argument aktualny kurs USD
        i na jego podstawie aktualizuje wartosci w kolumnie
        UnitPriceUSD
        '''

        UnitPrice = 6
        cursor = self.db.cursor()
    
        query = 'SELECT * FROM product'
        cursor.execute(query)
        logging.info(f"Executed SQL query: {query}")
        records = cursor.fetchall()

        for row in records:
            query = f"""UPDATE product SET UnitPriceUSD = {Decimal(currency)*row[UnitPrice]}
                        WHERE UnitPrice = {row[UnitPrice]}"""
            cursor.execute(query)

        self.db.commit()
        logging.info(f"Updated UnitPriceUSD collumn in {self.dbname} in table: product")
        cursor.close()

    def update_Euro_price(self, currency):

        '''
        Metoda przyjmuje jako argument aktualny kurs Euro
        i na jego podstawie aktualizuje wartosci w kolumnie
        UnitPriceEuro
        '''

        UnitPrice = 6

        cursor = self.db.cursor()
        
        query = 'SELECT * FROM product'
        cursor.execute(query)
        logging.info(f"Executed SQL query: {query}")
        records = cursor.fetchall()

        for row in records:
            
            query = f"""UPDATE product SET UnitPriceEuro = {Decimal(currency)*row[UnitPrice]}
                        WHERE UnitPrice = {row[UnitPrice]}"""
            cursor.execute(query)

        self.db.commit()
        logging.info(f"Updated UnitPriceEuro collumn in {self.dbname} in table: product")
        cursor.close()


    def make_excel_file(self):

        '''
        Metoda wykorzystuje modul pandas do wyslania
        zapytania do bazy danych i pobrania wszystkich
        wartosci tabeli 'product', ustawienia ich w 
        odpowiedniej kolejnosci i stworzeniu pliku .xlsx
        o nazwie zdefiniowanej w stalej EXCEL_FILE
        '''

        
        df = pd.read_sql("SELECT * FROM product", self.db, index_col='ProductID')

        cols = list(df.columns)

        df = df[cols[0:6]+cols[-2:]+cols[6:-3]]

        df.to_excel(EXCEL_FILE)

        logging.info(f"Created new excel file: {EXCEL_FILE}")
        

        


class NBPConnector():
    '''
    Klasa odpowiedzialna za caly proces laczenia
    sie z API NBP i pobieranie z niego odpowiednich
    wartosci.
    '''

    def __init__(self):
        self.api_base_url = 'http://api.nbp.pl/api/'
        self.tableA = 'a/'
        self.tableB = 'b/'
        self.tableC = 'c/'
        self.exchange_rates = 'exchangerates/rates/'
        self.today = 'today'

    logging.info("Created new NBPConnector")
     

    def get_updated_currency_by_code(self, currency_code):

        """
        Metoda przyjmuje kod w standardzie ISO 4217 i jesli
        taka waluta jest dostepna w bazie NBP to zostanie
        zwrocony jej aktualny kurs.
        """
            
        endpoint_today = f"{self.api_base_url}{self.exchange_rates}{self.tableA}{currency_code}{self.today}"
        endpoint = f"{self.api_base_url}{self.exchange_rates}{self.tableA}{currency_code}"
        
        status_code = 0
        
        try:
            r = requests.get(endpoint_today)
            logging.info(f"Send get request: {endpoint_today}")
            if r.status_code == 404:
                    r.raise_for_status()
        except HTTPError as err:
            status_code = err.response.status_code
            logging.error(f"Can't get today's {currency_code} exchange rate because: {status_code}")

        # jesli nie udalo sie pobrac "dzisiejszego" kursu
        # sprobuj pobrac ostatni 
        if status_code == 404:
            try:
                r = requests.get(endpoint)
                logging.info(f"Send get request: {endpoint}")
                if r.status_code == 404:
                    r.raise_for_status()
            except HTTPError as err:
                status_code = err.response.status_code
                logging.error(f"Can't get last {currency_code} exchange rate because: {status_code}")
                
                # jesli sie nie udalo pobrac ostatniego, zakoncz dzialanie metody
                # i zapisz log error
                return None
                

        logging.info(f"Recived the current {currency_code} exchange rate from the NBP API")
        data = r.json()

        currency = data['rates'][0]['mid']
        return currency

    

db = DBConnector(USER_NAME, PASSWORD, HOST, DATABASE)

db.connect_to_db()

nbp = NBPConnector()

usd_currency = nbp.get_updated_currency_by_code('usd')
euro_currency = nbp.get_updated_currency_by_code('eur')

db.update_Euro_price(euro_currency)
db.update_USD_price(usd_currency)

db.make_excel_file()

db.close_connection()
