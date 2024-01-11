from fastapi import FastAPI, HTTPException
import uvicorn
import sqlite3

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


