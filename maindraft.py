from fastapi import FastAPI, HTTPException
import uvicorn
import sqlite3

app = FastAPI()

db = "Chinook.db"
con = sqlite3.connect(db)

# Check if connexion with db is ok
# def validate_connexion_with_db():
#     try :
#         con
#         return con
#     except sqlite3.Error as e:
#         raise HTTPException(status_code=500, detail=str(e))

# Def validate_year as a year of 4 digits and integer. 
def validate_year(year: str): 
    if not year.isdigit() or not (len(year) == 4) :
        raise HTTPException(status_code=400, detail="The year should be a 4 digit number")
    return int(year)

# Def execute_result_query_SQL as cursor fetchall and result exception condition

def execute_result_query_SQL(con, query):
    cur=con.cursor()
    cur.execute(query)
    result = cur.fetchall()
    if result is None or len(result) == 0:
        raise HTTPException(status_code=400, detail="Your query has no result")
    if len(result) == 1 :
        return result[0][0]
    else:
        return {r[1]: r[0]  for r in result}

# Run the average income for a city and a year
    
@app.get("/average_income_per_year_city_draft/", description="Give the average income for a given year and city")
async def average_income_per_year_city(year: str, city: str):
    year = validate_year(year)
    req_inc_avg = f"SELECT revenu_fiscal_moyen, ville, date FROM foyers_fiscaux WHERE date LIKE '%{year}%' AND ville LIKE '{city}%'"
    return execute_result_query_SQL(con, req_inc_avg)

# @app.get("/transactions_per_city/")
# async def transactions_per_city(city: str):
#     #if not limit_number.isdigit() :
#     #    raise HTTPException(status_code=400, detail="le nombre de transaction doit être un chiffre")
#     #return int(limit_number)
#     city = city.upper()
    
#     cur = con.cursor()
#     req2 = f"SELECT * FROM transactions_sample WHERE ville LIKE '{city}%' ORDER BY date_transaction DESC LIMIT 10"
#     cur.execute(req2)
#     result2 = cur.fetchall()  # Récupère les résultats de la requête
#     if not result2 :
#         print("No data found for city")
#     else:
#         return result2
 
uvicorn.run(app)
con.close()  


# from fastapi import FastAPI, HTTPException
# import uvicorn
# import sqlite3

# app = FastAPI()

# db = "Chinook.db"
# con = sqlite3.connect(db)

# # Def validate_year as a year of 4 digits and integer.
 
# def validate_year(year: str): 
#     if not year.isdigit() or not (len(year) == 4) :
#         raise HTTPException(status_code=400, detail="The year should be a 4 digit number")
#     return int(year)

# # Def validate_number as an integer. 

# def validate_number(n: str): 
#     if not n.isdigit() :
#         raise HTTPException(status_code=400, detail="The given data is not an integer")
#     return int(n)

# # # Def validate_number as an integer. 

# # def validate_number(n: str): 
# #     if not n.isdigit() :
# #         raise HTTPException(status_code=400, detail="The given data is not an integer")
# #     return int(n)



# # Def execute_result_query_SQL as cursor fetchall and result exception condition

# def execute_result_query_SQL(con, query):
#     cur=con.cursor()
#     cur.execute(query)
#     result = cur.fetchall()
#     if result is None or len(result) == 0:
#         raise HTTPException(status_code=400, detail="Your query has no result")
#     return result #{r[1]: r[0]  for r in result}

# # Run the average income for a city and a year                                                                  ==> User Story 1
  
# @app.get("/average_income_per_year_city/", description="Give the average income for a given year and city")
# async def average_income_per_year_city(city: str, year: str):
#     year = validate_year(year)
#     req_inc_avg = f"SELECT revenu_fiscal_moyen, ville, date FROM foyers_fiscaux WHERE date LIKE '%{year}%' AND ville LIKE '{city}%'"
#     return execute_result_query_SQL(con, req_inc_avg)

# # Run the number of transactions per city in descending order limited by a number of lines                      ==> User Story 2

# @app.get("/transactions_per_city/", description="Give the complete transactions for a given city and to a limit number")
# async def transactions_per_city(city: str, limit_number: str):
#     limit = validate_number(limit_number)
#     city = city.upper()
#     req_transac = f"SELECT * FROM transactions_sample WHERE ville LIKE '{city}%' ORDER BY date_transaction DESC LIMIT {limit}"
#     return execute_result_query_SQL(con,req_transac)

# # Run the number of sells per city per year                                                                     ==> User Story 3

# @app.get("/sales_per_city/", description="Give the number of acquisitions for a given city and a given year")
# async def sales_per_city(city: str, year: str):
#     city = city.upper()
#     year = validate_year(year)
#     req_sales = f"SELECT COUNT(id_transaction) FROM transactions_sample WHERE ville LIKE '{city}%' AND date_transaction LIKE '{year}%'"
#     return execute_result_query_SQL(con,req_sales)

# #  Run the average price/squaremeter for the houses sold on a given year                                        ==> User Story 4

# @app.get("/average_price_m2_per_houses_per_year/", description="Give the the average price/squaremeter for the houses sold on a given year")
# async def avg_price_m2_houses(year: str):
#     year = validate_year(year)
#     req_avg_price_m2_houses = f"SELECT AVG(prix/surface_habitable) FROM transactions_sample WHERE date_transaction LIKE '{year}%' AND type_batiment LIKE 'Maison'"
#     return execute_result_query_SQL(con,req_avg_price_m2_houses)

# # Run the number of single room flat (studio) sold for a given city year                                        ==> User Story 5

# @app.get("/sales_studio_per_city/", description="Give the number of single room flat (studio) sold on a given city and year")
# async def sales_studio_per_city(city: str, year: str):
#     city = city.upper()
#     year = validate_year(year)
#     req_sales_studio_per_city = f"SELECT COUNT(*) FROM transactions_sample WHERE ville LIKE '{city}%' AND date_transaction LIKE '{year}%' AND n_pieces <2"
#     return execute_result_query_SQL(con,req_sales_studio_per_city)


# # Run the split of rooms per appartment and houses sold on a given city                                        ==> User Story 6

# @app.get("/sales_type_per_city/", description="Give the type of what king of goods have been sold on a given year and city")
# async def sales_type_per_city(city: str):
#     city = city.upper()
#     req_sales_type_per_room = f"SELECT ville, type_batiment, n_pieces, COUNT(*) FROM transactions_sample WHERE ville LIKE '{city}%'  GROUP BY n_pieces ORDER BY n_pieces"
#     return execute_result_query_SQL(con,req_sales_type_per_room)

# uvicorn.run(app)
# con.close()
