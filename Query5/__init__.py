import logging
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node
import os
import pyodbc as pyodbc
import azure.functions as func

# QUERY 5: Libre
# Language: SQL
def main(req: func.HttpRequest) -> func.HttpResponse:
    # Récupération du nom à chercher (param)
    logging.info('Python HTTP trigger function processed a request.')
    actor = req.params.get('name')
    genre = req.params.get('genre')
    director = req.params.get('director')
    if not actor or not genre or not director:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            actor = req_body.get('name')
            genre = req_body.get('genre')
            director = req_body.get('director')

    # Récupération des infos de connexion à la BDD
    server = os.environ["TPBDD_SERVER"]
    database = os.environ["TPBDD_DB"]
    username = os.environ["TPBDD_USERNAME"]
    password = os.environ["TPBDD_PASSWORD"]
    driver= '{ODBC Driver 17 for SQL Server}'

    if len(server)==0 or len(database)==0 or len(username)==0 or len(password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)
        
    errorMessage = ""
    dataString = ""
    avg_film = ""
 
    # Connexion à la BDD
    try:
        with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
            # Execution de la requête
            cursor = conn.cursor()
            cursor.execute("SELECT AVG(averageRating), genre FROM tGenres AS g JOIN tTitles AS t on t.tconst=g WHERE t ")

            rows = cursor.fetchall()
            for row in rows:
                avg_film += f"AVG: {row[0]}, genre: {row[1]}\n"

    except:
        errorMessage = "Erreur de connexion a la base SQL"
        
    # Renvoi du résultat (ou de l'erreur)
    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)
    else:
        return func.HttpResponse(avg_film)
