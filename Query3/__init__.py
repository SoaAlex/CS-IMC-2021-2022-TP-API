import logging
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node
import os
import pyodbc as pyodbc
import azure.functions as func

# QUERY 3: Affichez le noeud représentant l'acteur nommé Jude Law, et visualisez son année de naissance.
# Language: Neo4J (Cypher)
# Source: TP 1
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Récupération des infos de connexion à la BDD
    neo4j_server = os.environ["TPBDD_NEO4J_SERVER"]
    neo4j_user = os.environ["TPBDD_NEO4J_USER"]
    neo4j_password = os.environ["TPBDD_NEO4J_PASSWORD"]

    if len(neo4j_server)==0 or len(neo4j_user)==0 or len(neo4j_password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)
        
    errorMessage = ""
    dataString = ""
    birthYear = ""
 
    # Connexion à la BDD
    try:
        graph = Graph(neo4j_server, auth=(neo4j_user, neo4j_password))
        j_names = graph.run("MATCH (j:Name {primaryName: 'Jude Law'}) RETURN j.birthYear")
        for j_name in j_names:
            birthYear = j_name["j.birthYear"]

    except:
        errorMessage = "Erreur de connexion a la base Neo4J"
        
    # Renvoi du résultat (ou de l'erreur)
    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)
    else:
        return func.HttpResponse(f"Jude Law's birthYear is {birthYear} using Neo4J.")
