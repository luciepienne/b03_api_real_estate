from fastapi import FastAPI, HTTPException
import uvicorn
import sqlite3
# from funclist import validate_year, validate_city, city_exists, validate_build_type, validate_number, execute_result_query_SQL
# from fuzzywuzzy import fuzz #to find most similar building types see def validate_build_type 

app = FastAPI()

db = "Chinook.db"
con = sqlite3.connect(db)

# Def validate_year as a year of 4 digits and integer. 
def validate_year(year: str): 
    if not year.isdigit() or not (len(year) == 4) :
        raise HTTPException(status_code=400, detail="The year should be a 4 digit number")
    return year

# Def validate_number as an integer. 
def validate_number(n: str): 
    if not n.isdigit() :
        raise HTTPException(status_code=400, detail="The given data is not an integer")
    return int(n)

# Def valid building type as appartement or maison or looking like. 
def validate_build_type(building_type: str): 
    valid_building_types = ['APPARTEMENT', 'MAISON']
    #most_similar_building_type = max(valid_building_types, key=lambda x: fuzz.ratio(building_type.upper(), x))
    if building_type.upper() not in valid_building_types:
        raise HTTPException(status_code=400, detail="The given type is not an 'appartement' or a 'maison'")
    return building_type.upper() #return most_similar_building_type

# Def execute_result_query_SQL as cursor fetchall and result exception condition

def execute_result_query_SQL(con, query):
    cur=con.cursor()
    cur.execute(query)
    result = cur.fetchall()
    print(query)
    print(result)
    if result is None or len(result) == 0:
        raise HTTPException(status_code=404, detail="Your query has no result")
    if len(result) == 1:
        return result[0]
    else:
        return result #return result{r[1]: r[0]  for r in result} to get second column and 1st column of the result

# Def check if city exists in the table and return the name

def city_exists(con, city, table):
    query = f"SELECT COUNT(*) FROM {table} WHERE UPPER(ville) LIKE '{city.upper()}%'"
    result = execute_result_query_SQL(con, query)
    return result[0] > 0


# Def return error if city doesn't exist in the table

def validate_city(con, city, table):
    if not city_exists(con, city, table):
        raise HTTPException(status_code=400, detail="The city indicated has no data")
    return city.upper()


# Run the average income for a city and a year                                                                  ==> User Story 1
  
@app.get("/average_income_per_year_city/", description="Give the average income for a given year and city")
async def average_income_per_year_city(city: str, year: str):
    year = validate_year(year)
    validate_city(con,city,"foyers_fiscaux")
    req_inc_avg = f"SELECT revenu_fiscal_moyen, ville, date FROM foyers_fiscaux WHERE date LIKE '%{year}%' AND ville LIKE UPPER('{city}%')"
    return execute_result_query_SQL(con, req_inc_avg)

# Run the number of transactions per city in descending order limited by a number of lines                      ==> User Story 2

@app.get("/transactions_per_city/", description="Give the complete transactions for a given city and to a limit number")
async def transactions_per_city(city: str, limit_number: str):
    limit = validate_number(limit_number)
    validate_city(con,city,"transaction_sample")
    req_transac = f"SELECT * FROM transactions_sample WHERE ville LIKE '{city}%' ORDER BY date_transaction DESC LIMIT {limit}"
    return execute_result_query_SQL(con,req_transac)

# Run the number of sells per city per year                                                                     ==> User Story 3

@app.get("/sales_per_city/", description="Give the number of acquisitions for a given city and a given year")
async def sales_per_city(city: str, year: str):
    city = city.upper()
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

# Run the number of single room flat (studio) sold for a given city year                                        ==> User Story 5

@app.get("/sales_studio_per_city/", description="Give the number of single room flat (studio) sold on a given city and year")
async def sales_studio_per_city(city: str, year: str):
    city = city.upper()
    year = validate_year(year)
    req_sales_studio_per_city = f"SELECT COUNT(*) FROM transactions_sample WHERE ville LIKE '{city}%' AND date_transaction LIKE '{year}%' AND n_pieces <2"
    return execute_result_query_SQL(con,req_sales_studio_per_city)


# Run the split of rooms per appartment and houses sold on a given city                                        ==> User Story 6

@app.get("/sales_type_per_city/", description="Give the type of what king of goods have been sold on a given year and city")
async def sales_type_per_city(city: str):
    city = city.upper()
    req_sales_type_per_room = f"SELECT n_pieces, COUNT(*) FROM transactions_sample WHERE ville LIKE '{city}%' AND type_batiment LIKE 'Appartement' OR 'Maison' GROUP BY n_pieces ORDER BY n_pieces"
    return execute_result_query_SQL(con,req_sales_type_per_room)

if __name__ == '__main__':
    uvicorn.run(app) # uniquement si lancement en python