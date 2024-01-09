from fastapi import FastAPI, HTTPException
import uvicorn
import sqlite3

app = FastAPI()

db = "Chinook.db"
con = sqlite3.connect(db)

# we validate the figure of the year and make sure it is a 4 digits and integer. 
# # The raise HTTPException is mandatory to avoid crashing the system with data not ok.

def validate_year(year: str): 
    if not year.isdigit() or not (len(year) == 4) :
        raise HTTPException(status_code=400, detail="L'année doit être une valeur numérique de 4 chiffres")
    return int(year)
    
@app.get("/revenu_fiscal_moyen/")
async def revenu_fiscal_moyen(year: str, city: str):
    # Utilisez la valeur validée de l'année dans votre logique de traitement
    year = validate_year(year)
    cur = con.cursor()
    req = f"SELECT revenu_fiscal_moyen, date, ville FROM foyers_fiscaux WHERE date LIKE '%{year}%' AND ville LIKE '{city}%'"
    cur.execute(req)    
    result = cur.fetchall()  # Récupère les résultats de la requête
    if result is None or len(result) == 0:
        raise HTTPException(status_code=400, detail='Aucune valeur')
    if len(result) == 1:
        return result[0][0]
    else:
        return {r[2]: r[0] for r in result}
    #if result is None:
    #    return  "No data found for city {city} in year {year}"
    #else:
     #   return {r[2]: r[0] for r in result}
 #con.close()  # Ferme la connexion à la base de données 


@app.get("/transactions_per_city/")
async def transactions_per_city(city: str):
    #if not limit_number.isdigit() :
    #    raise HTTPException(status_code=400, detail="le nombre de transaction doit être un chiffre")
    #return int(limit_number)
    city = city.upper()
    cur = con.cursor()
    req2 = f"SELECT * FROM transactions_sample WHERE ville LIKE '{city}%' ORDER BY date_transaction DESC LIMIT 10"
    cur.execute(req2)
    result2 = cur.fetchall()  # Récupère les résultats de la requête
    if result2 is None:
        return "No data found for city"
    else:
        return result2
    
uvicorn.run(app)



# #         Ma version qui ne fonctionne pas ...from fastapi import FastAPI
#             import uvicorn

#             app = FastAPI()

#             @app.get("/revenu_fiscal_moyen/")
#             async def revenu_fiscal_moyen(year: int, city: str):
#             if year is > 1900 OR year is < DATEADD(year, -1, GETDATE()) :
#             then return f"SELECT revenu_fiscal_moyen, date, ville FROM foyers_fiscaux WHERE date LIKE '{year}' AND ville LIKE '{city}'"
#             else raise ("L'année n'est pas au bon format ou ne renvoie pas de données")
