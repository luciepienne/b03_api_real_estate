from fastapi import FastAPI, HTTPException
import uvicorn
import sqlite3
from enum import Enum
from funclist import validate_year, validate_city, city_exists, validate_build_type, validate_number, execute_result_query_SQL
# from fuzzywuzzy import fuzz #to find most similar building types see def validate_build_type 

app = FastAPI(title="REAL ESTATE DATA REQUESTS")

db = "Chinook.db"
con = sqlite3.connect(db)

class Type(Enum):
    MAISON = "Maison"
    APPARTEMENT = "Appartement"
      

# Run the average income for a city and a year                                                                  ==> User Story 1
  
@app.get("/average_income_per_year_city/", description="Give the average income for a given year and city")
async def average_income_per_year_city(city: str, year: str):
    year = validate_year(year)
    city = validate_city(con,city,"foyers_fiscaux")
    req_inc_avg = f"SELECT ville, revenu_fiscal_moyen, date FROM foyers_fiscaux WHERE date LIKE '%{year}%' AND ville LIKE UPPER('{city}%')"
    return execute_result_query_SQL(con, req_inc_avg)

# Run the number of transactions per city in descending order limited by a number of lines                      ==> User Story 2

@app.get("/transactions_per_city/", description="Give the complete transactions for a given city and to a limit number")
async def transactions_per_city(city: str, limit_number: str =""):
    city = validate_city(con,city,"transactions_sample")
    if limit_number == "":
        limit = 10 
    else:
        limit = validate_number(limit_number)
    req_transac = f"SELECT * FROM transactions_sample WHERE ville LIKE '{city}%' ORDER BY date_transaction DESC LIMIT {limit}"
    return execute_result_query_SQL(con,req_transac)

# Run the number of sells per city per year                                                                     ==> User Story 3

@app.get("/sales_per_city/", description="Give the number of acquisitions for a given city and a given year")
async def sales_per_city(city: str, year: str):
    city = validate_city(con,city,"transactions_sample")
    year = validate_year(year)
    req_sales = f"SELECT COUNT(id_transaction) FROM transactions_sample WHERE ville LIKE '{city}%' AND date_transaction LIKE '{year}%'"
    return execute_result_query_SQL(con,req_sales)

#  Run the average price/squaremeter for the building type sold on a given year                                  ==> User Story 4

@app.get("/average_price_m2_per_type_per_year/", description="Give the average price/squaremeter sold on a given year for a given type of buildings")
async def avg_price_m2_per_type(buiding_type: str, year: str):
    year = validate_year(year)
    building_type = validate_build_type(buiding_type)
    req_avg_price_m2_per_type = f"SELECT type_batiment, AVG(prix/surface_habitable) FROM transactions_sample WHERE date_transaction LIKE '{year}%' AND UPPER(type_batiment) = '{building_type}'"
    return execute_result_query_SQL(con,req_avg_price_m2_per_type)

#  Run the average price/squaremeter for the building type (filter) sold on a given year                        ==> User Story 4Bis

@app.get("/average_price_m2_per_type_per_year_city/", description="Permet de connaitre le prix par m² par type de logement et par année")
async def prix_m2(type:Type, city: str, year: str):
    year = validate_year(year)
    city = validate_city(con,city,"transactions_sample")
    req_avg_price_m2_per_type_class= f"SELECT ville, type_batiment, AVG(prix/surface_habitable) FROM transactions_sample WHERE ville LIKE '{city}%' AND UPPER(type_batiment) LIKE UPPER('{type.value}') AND date_transaction LIKE '{year}%';"
    return execute_result_query_SQL(con,req_avg_price_m2_per_type_class)

# Run the number of single room flat (studio) sold for a given city year                                        ==> User Story 5

@app.get("/sales_studio_per_city/", description="Give the number of single room flat (studio) sold on a given city and year")
async def sales_studio_per_city(city: str, year: str):
    city = validate_city(con,city,"transactions_sample")
    year = validate_year(year)
    req_sales_studio_per_city = f"SELECT COUNT(*) FROM transactions_sample WHERE ville LIKE '{city}%' AND date_transaction LIKE '{year}%' AND n_pieces <2"
    return execute_result_query_SQL(con,req_sales_studio_per_city)


# Run the split of rooms per appartment and houses sold on a given city                                        ==> User Story 6

@app.get("/sales_type_per_city/", description="Give the type of what king of goods have been sold on a given year and city")
async def sales_type_per_city(city: str):
    city = validate_city(con,city,"transactions_sample")
    req_sales_type_per_room = f"SELECT ville, type_batiment, n_pieces, COUNT(*) FROM transactions_sample WHERE ville LIKE '{city}%' AND type_batiment LIKE 'Appartement' OR 'Maison' GROUP BY n_pieces ORDER BY n_pieces"
    return execute_result_query_SQL(con,req_sales_type_per_room)


if __name__ == '__main__':
    uvicorn.run(app) # uniquement si lancement en python