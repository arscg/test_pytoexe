# -*- coding: utf-8 -*-
"""
Created on Mon Jan  6 14:10:34 2025

@author: Utilisateur
"""

import sqlite3

def init_database(database):
    """Initialise la base de données en supprimant les tables existantes."""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    
    # Suppression des tables si elles existent déjà
    cursor.execute("DROP TABLE IF EXISTS aggregate_bague_source;")
    cursor.execute("DROP TABLE IF EXISTS conso_indiv_semaine_mangeoire;")
    cursor.execute("DROP TABLE IF EXISTS conso_indiv_semaine_parquet;")
    cursor.execute("DROP TABLE IF EXISTS bague_heure;")
    conn.commit()  # Sauvegarde des modifications
    
    conn.close()

def creer_table_aggregate_bague_source(database):
    """Crée la table 'aggregate_bague_source' si elle n'existe pas déjà."""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
CREATE TABLE aggregate_bague_source AS
SELECT 
    bague, 
    REPLACE(GROUP_CONCAT(DISTINCT source ORDER BY source), ',', '/') AS sources_concatenées
FROM results
GROUP BY bague;
""")
        conn.commit()  # Sauvegarde des modifications
    except sqlite3.OperationalError as e:
        if "already exists" in str(e):
            print("La table aggregate_bague_source existe déjà.")
        else:
            raise
    
    conn.close()


def creer_table_bague_heure(database):
    """Crée la table 'utilisation_mangeoires' si elle n'existe pas déjà."""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
CREATE TABLE bague_heure AS
SELECT
	parquet,
    mangeoire,
	evenement,
	bague,
	sources_concatenées,
	jour, 
    heure,
    semaine,
	sum(conso_g) as quantite_grammes,	
    sum(compt) as nombre_prises,
    sum(duree_s) as duree_prises
FROM
	(SELECT 
	    r.*, 
	    abs.bague,
	    abs.sources_concatenées
	FROM 
	    results r
	INNER JOIN 
	    aggregate_bague_source abs ON r.bague = abs.bague)
GROUP BY	parquet,
            mangeoire,
        	evenement,
        	bague,
        	sources_concatenées,
        	jour,
            heure,
            semaine;
""")
        conn.commit()  # Sauvegarde des modifications
    except sqlite3.OperationalError as e:
        if "already exists" in str(e):
            print("La table utilisation_parquet existe déjà.")
        else:
            raise
    
    conn.close()
    
def creer_vue_heure(colonne, database):
    """Crée une vue pour une colonne spécifique."""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    
    try:
        query_parts = [
            f"SUM(CASE WHEN heure = {h} THEN {colonne} END) AS H_{h:02d}_{colonne}"
            for h in range(24)
        ]
        
        query = f"""
CREATE VIEW IF NOT EXISTS vue_heure_{colonne} AS
SELECT 
    parquet, mangeoire, evenement, bague, sources_concatenées, jour,
    {', '.join(query_parts)}
FROM 
    bague_heure
GROUP BY 
    parquet, mangeoire, evenement, bague, sources_concatenées, jour;
"""
        
        cursor.execute(query)
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"Erreur lors de la création de la vue 'vue_mangeoire_heure_{colonne}' : {e}")
    
    conn.close()
    
def creer_vue_heure_parquet(colonne, database):
    """Crée une vue pour une colonne spécifique."""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    
    try:
        query_parts = [
            f"SUM(CASE WHEN heure = {h} THEN {colonne} END) AS H_{h:02d}_{colonne}"
            for h in range(24)
        ]
        
        query_parts_2 = [
            f"SUM(H_{(h):02d}_{colonne}) as  H_{(h):02d}_{colonne}"
            for h in range(24)
        ]
        
        query = f"""
CREATE VIEW IF NOT EXISTS vue_heure_parquet_{colonne} AS
SELECT parquet,
REPLACE(GROUP_CONCAT(DISTINCT mangeoires ORDER BY mangeoires), ',', '/') AS mangeoires,
evenement, bague, 
sources_concatenées, 
jour,
       {', '.join(query_parts_2)}
FROM (
    SELECT 
        parquet,
    		REPLACE(GROUP_CONCAT(DISTINCT mangeoire ORDER BY mangeoire), ',', '/') AS mangeoires,
    		evenement, bague, 
    		sources_concatenées, 
    		jour, 
        {', '.join(query_parts)}
    FROM 
        bague_heure
    GROUP BY 
        parquet, evenement, bague, jour
) AS a
GROUP BY parquet, evenement, bague, jour;
"""
        cursor.execute(query)
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"Erreur lors de la création de la vue 'vue_mangeoire_heure_{colonne}' : {e}")
    
    conn.close()

def creer_vue_jour(colonne, database):
    """Crée une vue pour une colonne spécifique."""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    
    try:
        """Crée une vue pour une colonne spécifique."""
        conn = sqlite3.connect(database)
        cursor = conn.cursor()
        
        # Récupération du maximum de la colonne 'semaine'
        cursor.execute("SELECT MAX(jour) FROM bague_heure")
        max_jour = cursor.fetchone()[0]
       
        if max_jour is None:
           print("La table 'bague_heure' est vide.")
           return
       
        query_parts = [
            f"SUM(CASE WHEN jour = {j+1} THEN {colonne} END) AS J_{(j+1):02d}_{colonne}"
            for j in range(max_jour)
        ]
        
        query = f"""
CREATE VIEW IF NOT EXISTS vue_jour_{colonne} AS
SELECT 
    parquet, mangeoire, evenement, bague, sources_concatenées,
    {', '.join(query_parts)}
FROM 
    bague_heure
GROUP BY 
    parquet, mangeoire, evenement, bague, sources_concatenées;
"""
        
        cursor.execute(query)
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"Erreur lors de la création de la vue 'vue_mangeoire_heure_{colonne}' : {e}")
    
    conn.close()
    
def creer_vue_jour_parquet(colonne, database):
    """Crée une vue pour une colonne spécifique."""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    
    # Récupération du maximum de la colonne 'semaine'
    cursor.execute("SELECT MAX(jour) FROM bague_heure")
    max_jour = cursor.fetchone()[0]
   
    if max_jour is None:
       print("La table 'bague_heure' est vide.")
       return
   
    query_parts = [
        # f"MAX(CASE WHEN jour = {s+1} THEN {colonne} END) AS s_{(s+1):02d}_{colonne}"
        f"SELECT SUM(CASE WHEN jour = {s+1} THEN {colonne} END) AS s_{(s+1):02d}_{colonne}"
        for s in range(max_jour)
    ]
    
    try:
        query_parts = [
            f"SUM(CASE WHEN jour = {j+1} THEN {colonne} END) AS J_{(j+1):02d}_{colonne}"
            for j in range(max_jour)
        ]
        
        query_parts_2 = [
            f"SUM(J_{(j+1):02d}_{colonne}) AS J_{(j+1):02d}_{colonne}"
            for j in range(max_jour)
        ]
        
        query = f"""
CREATE VIEW IF NOT EXISTS vue_jour_parquet_{colonne} AS
SELECT parquet,
       evenement,
       sources_concatenées,
       REPLACE(GROUP_CONCAT(DISTINCT mangeoire ORDER BY mangeoire), ',', '/') AS mangeoires,
        {', '.join(query_parts_2)}
        
from
    (SELECT parquet, 
           mangeoire, 
           evenement,
           sources_concatenées, 
           {', '.join(query_parts)}
    FROM bague_heure
    GROUP BY parquet, 
             mangeoire, 
             sources_concatenées, 
             evenement
) AS a
GROUP BY parquet,  
         sources_concatenées, 
         evenement;

"""
        
        cursor.execute(query)
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"Erreur lors de la création de la vue 'vue_mangeoire_heure_{colonne}' : {e}")
    
    conn.close()
    

def creer_vue_jour_parquet_(colonne, database):
    """Crée une vue pour une colonne spécifique."""
    
    # Connexion à la base de données
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    
    try:
        # Récupération du maximum de la colonne 'jour'
        cursor.execute("SELECT MAX(jour) FROM bague_heure")
        max_jour = cursor.fetchone()[0]
        
        if max_jour is None:
            print("La table 'bague_heure' est vide.")
            return
        
        # Création des parties de la requête SQL
        query_parts = [
            f"SUM(CASE WHEN jour = {j+1} THEN {colonne} END) AS J_{(j+1):02d}_{colonne}"
            for j in range(max_jour)
        ]
        
        query_parts_2 = [
            f"SUM(J_{(j+1):02d}_{colonne}) as  J_{(j+1):02d}_{colonne}"
            for j in range(max_jour)
        ]
        
        # Construction complète de la requête SQL
        query = f"""
CREATE VIEW IF NOT EXISTS vue_jour_parquet_{colonne} AS
SELECT parquet,
       evenement,
       sources_concatenées,
       REPLACE(GROUP_CONCAT(DISTINCT mangeoire ORDER BY mangeoire), ',', '/') AS mangeoires,
       {', '.join(query_parts_2)}
FROM (
    SELECT parquet, 
           mangeoire, 
           evenement,
           sources_concatenées, 
           {', '.join(query_parts)}
    FROM bague_heure
    GROUP BY parquet, 
             mangeoire, 
             sources_concatenées, 
             evenement
) AS a
GROUP BY parquet,  
         sources_concatenées, 
         evenement;
"""
        
        # Exécution de la requête SQL
        cursor.execute(query)
        
        # Validation des modifications
        conn.commit()
    except sqlite3.Error as e:
        print(f"Erreur lors de la création de la vue 'vue_jour_parquet_{colonne}' : {e}")
    finally:
        # Fermeture de la connexion à la base de données
        cursor.close()
        conn.close()

def creer_vue_semaine(colonne, database):
    """Crée une vue pour une colonne spécifique."""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    
    try:
        # Récupération du maximum de la colonne 'semaine'
        cursor.execute("SELECT MAX(semaine) FROM bague_heure")
        max_semaine = cursor.fetchone()[0]
       
        if max_semaine is None:
           print("La table 'bague_heure' est vide.")
           return
       
        query_parts = [
            f"MAX(CASE WHEN jour = {s+1} THEN {colonne} END) AS s_{(s+1):02d}_{colonne}"
            for s in range(max_semaine)
        ]
        # Récupération du maximum de la colonne 'semaine'
        cursor.execute("SELECT MAX(semaine) FROM bague_heure")
        max_semaine = cursor.fetchone()[0]
       
        if max_semaine is None:
           print("La table 'bague_heure' est vide.")
           return
       
        query_parts = [
            f"SUM(CASE WHEN semaine = {s+1} THEN {colonne} END) AS S_{(s+1):02d}_{colonne}"
            for s in range(max_semaine)
        ]
        
        query = f"""
CREATE VIEW IF NOT EXISTS vue_semaine_{colonne} AS
SELECT 
    parquet, mangeoire, evenement, bague, sources_concatenées,
    {', '.join(query_parts)}
FROM 
    bague_heure
GROUP BY 
    parquet, mangeoire, evenement, bague, sources_concatenées;
"""
        
        cursor.execute(query)
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"Erreur lors de la création de la vue 'vue_mangeoire_heure_{colonne}' : {e}")
    
    conn.close()
    
def creer_vue_semaine_parquet(colonne, database):
    """Crée une vue pour une colonne spécifique."""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    
    try:
        # Récupération du maximum de la colonne 'semaine'
        cursor.execute("SELECT MAX(semaine) FROM bague_heure")
        max_semaine = cursor.fetchone()[0]
       
        if max_semaine is None:
           print("La table 'bague_heure' est vide.")
           return
       
        query_parts = [
            f"SUM(CASE WHEN semaine = {s+1} THEN {colonne} END) AS S_{(s+1):02d}_{colonne}"
            for s in range(max_semaine)
        ]
        
        query_parts_2 = [
            f"SUM(S_{(s+1):02d}_{colonne}) as  S_{(s+1):02d}_{colonne}"
            for s in range(max_semaine)
        ]

        query = f"""
CREATE VIEW IF NOT EXISTS vue_semaine_parquet_{colonne} AS
SELECT parquet,
REPLACE(GROUP_CONCAT(DISTINCT mangeoires ORDER BY mangeoires), ',', '/') AS mangeoires,
evenement, bague, 
sources_concatenées, 
    {', '.join(query_parts_2)}
FROM (
    SELECT 
        parquet,
    		REPLACE(GROUP_CONCAT(DISTINCT mangeoire ORDER BY mangeoire), ',', '/') AS mangeoires,
    		evenement, bague, 
    		sources_concatenées, 
            semaine,
         {', '.join(query_parts)}
    FROM 
        bague_heure
    GROUP BY 
        parquet, evenement, bague
) AS a
GROUP BY parquet, evenement, bague;
"""
        cursor.execute(query)
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"Erreur lors de la création de la vue 'vue_mangeoire_heure_{colonne}' : {e}")
        
    conn.close()
    
    
def database_construct(database):
    init_database(database)
    creer_table_aggregate_bague_source(database)
    creer_table_bague_heure(database)
    creer_vue_heure('quantite_grammes', database)
    creer_vue_heure('nombre_prises', database)
    creer_vue_heure('duree_prises', database)
    creer_vue_heure_parquet('quantite_grammes', database)
    creer_vue_heure_parquet('nombre_prises', database)
    creer_vue_heure_parquet('duree_prises', database)
    creer_vue_jour('quantite_grammes', database)
    creer_vue_jour('nombre_prises', database)
    creer_vue_jour('duree_prises', database)
    creer_vue_jour_parquet('quantite_grammes', database)
    creer_vue_jour_parquet('nombre_prises', database)
    creer_vue_jour_parquet('duree_prises', database)
    creer_vue_semaine('quantite_grammes', database)
    creer_vue_semaine('nombre_prises', database)
    creer_vue_semaine('duree_prises', database)
    creer_vue_semaine_parquet('quantite_grammes', database)
    creer_vue_semaine_parquet('nombre_prises', database)
    creer_vue_semaine_parquet('duree_prises', database)
