from fastapi import FastAPI
import uvicorn

app = FastAPI()

@app.get("/revenu_fiscal_moyen/")
async def revenu_fiscal_moyen(year: int, city: str):
    return f"SELECT revenu_fiscal_moyen, date, ville FROM foyers_fiscaux WHERE date LIKE '{year}' AND ville LIKE '{city}'"
@app.get("/transactions_par_ville/")
async def transactions_par_ville(year: int, city: str):
    return f"SELECT COUNT(id_transaction) FROM transactions_sample WHERE ville LIKE '{city}' AND date_transaction LIKE '{year}'"

