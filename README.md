# Update Price Script

The script connects to the database to update the values of
columns with prices in USD and Euro.
New value is calculated based on UnitPrice column and the 
current currency rate taken from the NBP API.

Script also have method for export data of table 'products'
to excel file. 

## Technologies Used

### Main:
- Python - version 3.8.7

### Modules
- requests
- mysql.connector
- pandas
- openpyxl

## Setup

Script requires the following modules to be installed:

### requests

    `pip install `

### mysql.connector

    `pip install mysql-connector-python`

### pandas

    `pip install pandas`

### openpyxl

    `pip install openpyxl`

## Usage 

Download main.py and mydb files. 
Import database. 
Fill in the variables with your config.
Run the script.