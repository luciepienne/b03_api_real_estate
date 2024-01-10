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



