from fastapi import FastAPI, HTTPException, Depends
import uvicorn
import sqlite3

app = FastAPI()

# we validate the figure of the year and make sure it is a 4 digits and integer. 
    # The raise HTTPException is mandatory to avoid crashing the system with data not ok.

def validate_year(year: str): 
    if not year.isdigit() or not year > 1980 or not year < DATEADD(year, -1, GETDATE()) : #(len(year) == 4) :
        raise HTTPException(status_code=400, detail="L'année doit être une valeur numérique de 4 chiffres")

    return int(year)

@app.get("/revenu_fiscal_moyen/")
async def revenu_fiscal_moyen(year: str, city: str = ""):
    # Utilisez la valeur validée de l'année dans votre logique de traitement
    year = validate_year(year)
    return f"SELECT revenu_fiscal_moyen, date, ville FROM foyers_fiscaux WHERE date LIKE '{year}' AND ville LIKE '{city}'"


@app.get("/transactions_par_ville/")
async def transactions_par_ville(year: int, city: str):
    return f"SELECT COUNT(id_transaction) FROM transactions_sample WHERE ville LIKE '{city}' AND date_transaction LIKE '{year}'"


uvicorn.run(app)


# #         Ma version qui ne fonctionne pas ...from fastapi import FastAPI
#             import uvicorn

#             app = FastAPI()

#             @app.get("/revenu_fiscal_moyen/")
#             async def revenu_fiscal_moyen(year: int, city: str):
#             if year is > 1900 OR year is < DATEADD(year, -1, GETDATE()) :
#             then return f"SELECT revenu_fiscal_moyen, date, ville FROM foyers_fiscaux WHERE date LIKE '{year}' AND ville LIKE '{city}'"
#             else raise ("L'année n'est pas au bon format ou ne renvoie pas de données")

