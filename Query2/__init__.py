import logging
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node
import os
import pyodbc as pyodbc
import azure.functions as func

# QUERY 2: Visualisez l'année de naissance de l'artiste Jude Law
# Language: SQL
# Source: TP 1
def main(req: func.HttpRequest) -> func.HttpResponse:
    # Récupération des infos de connexion à Azure
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

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
    birthYear = ""

    # Récupération du nom à rechercher dans la BDD
    if name:
        nameMessage = f"Hello, {name}!\n"
    else:
        nameMessage = "Le parametre 'name' n'a pas ete fourni lors de l'appel.\n"
 
    # Connexion à la BDD
    try:
        with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
            # Execution de la requête
            cursor = conn.cursor()
            cursor.execute(f"SELECT birthYear FROM tNames WHERE primaryName = '{name}'")

            rows = cursor.fetchall()
            for row in rows:
                birthYear = row[0]

    except:
        errorMessage = "Erreur de connexion a la base SQL"
        
    # Renvoi du résultat (ou de l'erreur)
    if errorMessage != "":
        return func.HttpResponse(dataString + nameMessage + errorMessage, status_code=500)
    else:
        return func.HttpResponse(f"{name}'s birthYear is {birthYear}.")
