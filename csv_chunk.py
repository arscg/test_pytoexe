# -*- coding: utf-8 -*-
"""
Created on Wed Dec 25 11:53:42 2024

@author: Utilisateur
"""

import pandas as pd
from datetime import datetime
import os
import time
import gc
import yaml

# Importer la bibliothèque sqlite3
import sqlite3

def calculer_stats_numeriques(df):
    """
    Calcule les statistiques pour les colonnes numériques.

    Args:
        df (pd.DataFrame): DataFrame pour lequel calculer les statistiques.

    Returns:
        stats (dict): Dictionnaire contenant les statistiques.
    """    
    # Créer un dictionnaire vide pour stocker les statistiques
    stats = {}
    
    # Utiliser la méthode describe() pour obtenir les statistiques numériques
    stats_num = df[['avant_g', 'apres_g', 'conso_g', 'duree_s']].describe()
    
    return {'numerique': stats_num}

def calculer_stats_non_numeriques_par_colonne(df):
    """
    Calcule les statistiques pour les colonnes non numériques.

    Args:
        df (pd.DataFrame): DataFrame pour lequel calculer les statistiques.

    Returns:
        stats (dict): Dictionnaire contenant les statistiques.
    """

    # Créer un dictionnaire vide pour stocker les statistiques
    stats = {}

    for colonne in df.columns:
        if not pd.api.types.is_numeric_dtype(df[colonne]):
            stats[colonne] = {}
            
            stats[colonne]['value_counts'] = df[colonne].value_counts()
            stats[colonne]['unique'] = df[colonne].nunique()
            stats[colonne]['null'] = df[colonne].isnull().sum()

    return {'non_numerique_par_colonne': stats}

def calculer_stats(df, chemin_fichier):
    """
    Calcule les statistiques pour un DataFrame.

    Args:
        df (pd.DataFrame): DataFrame pour lequel calculer les statistiques.
        chemin_fichier (str): Chemin du fichier CSV traité.

    Returns:
        stats (dict): Dictionnaire contenant les statistiques.
    """
    
    # Calculer les statistiques numériques
    stats_numerique = calculer_stats_numeriques(df)
    
    # Calculer les statistiques non numériques par colonne
    stats_non_numerique_par_colonne = calculer_stats_non_numeriques_par_colonne(df)
    
    return {'numerique': stats_numerique, 'non_numerique': stats_non_numerique_par_colonne, 'fichier': chemin_fichier }

def convertir_date_locale_en_timestamp(df):
    """
    Convertit la colonne date_locale en timestamp numérique.

    Args:
        df (pd.DataFrame): DataFrame contenant la colonne date_locale à convertir.

    Returns:
        df (pd.DataFrame): DataFrame avec la nouvelle colonne timestamp ajoutée.
    """
    
    # Créer une nouvelle colonne timestamp en utilisant la fonction to_datetime
    df['timestamp'] = pd.to_datetime(df['date_locale'], dayfirst=True)
    
    # Convertir la colonne timestamp en timestamp numérique (en secondes depuis l'epoch)
    df['timestamp_num'] = df['timestamp'].apply(lambda x: int(x.timestamp()))
    
    return df

def traiter_fichier(chemin_fichier, date, parquets, bague):
    """
    Traite un fichier CSV et renvoie le DataFrame correspondant.

    Args:
        chemin_fichier (str): Chemin du fichier CSV à traiter.
        date (int): Timestamp numérique de la date à utiliser pour les calculs.
        parquets (dict): Dictionnaire de correspondance entre 'mangeoire' et 'parquet'.

    Returns:
        df (pd.DataFrame): DataFrame résultant de la lecture du fichier CSV.
        stats_apres (dict): Dictionnaire contenant les statistiques après traitement.
    """

    # Mesurer le temps d'exécution pour ce fichier
    start_time_file = datetime.now().timestamp()  
    
    # Lecture du fichier CSV en utilisant la séparation virgulee
    df = pd.read_csv(chemin_fichier, sep=',')
    
    # Appliquer la fonction de conversion date_locale -> timestamp
    df = convertir_date_locale_en_timestamp(df)
    
    # Ajout d'une colonne avec le nom du fichier
    df['Fichier'] = chemin_fichier
   
    # Ajouter la nouvelle colonne parquet en utilisant la liste de correspondance
    df['parquet'] = df['mangeoire'].map(parquets)
    df['bague'] = df['source'].map(bague)
    
    # Ajout de la colonne delta pour le jour et la semaine
    df['jour'] = (pd.to_timedelta(df['timestamp_num'] - date, unit='s').dt.days) + 1
    df['semaine'] = ((pd.to_timedelta(df['timestamp_num'] - date, unit='s').dt.days)//7) + 1
    
    # Supprimer les colonnes inutiles
    df = df.drop(columns=['date_locale', 'timestamp', 'timestamp_num'])
    
    df['heure'] = df['heures'].str.split(':').str[0]
    
    
    # Groupement du DataFrame par les colonnes spécifiées
    grouped_df = df.groupby(['date', 'mangeoire', 'evenement', 'source', 'bague', 'Fichier', 'parquet', 'heure',  'jour', 'semaine'])
    
    # Calcul des sommes pour les colonnes spécifiées
    result_df = grouped_df[['avant_g', 'apres_g', 'conso_g', 'duree_s']].sum().reset_index()
    
    # Calcul du nombre de lignes par groupe
    result_df['compt'] = grouped_df.size().reset_index(name='nombre_lignes')['nombre_lignes']

    # Calculer les statistiques après traitement
    stats_apres = calculer_stats(df, chemin_fichier )
    
    # Enregistrer le résultat dans une base de données SQLite
    enregistrer_resultats(result_df, 'results_chunks.db')
    
    return result_df, stats_apres


def main():
    """
    Fonction principale qui traite tous les fichiers CSV dans les répertoires spécifiés.
    """
    
    start_time_total = time.time()  # Mesurer le temps d'exécution total
    
    chemin_dossier = r'.\CSV\chunks'  # Chemin du dossier contenant les fichiers CSV
    
    date = recuperer_date_yaml( os.path.join(chemin_dossier, 'comfig.yaml'))  # Récupérer la date à partir du fichier YAML
    parquet = recuperer_parquet_yaml( os.path.join(chemin_dossier, 'comfig.yaml'))
    bague = recuperer_bague_yaml( os.path.join(chemin_dossier, 'comfig.yaml'))
    
    print (date)

    results = []
    
    for i in range(1, 5):  # boucle sur M01 à M04
        start_time_file = time.time()
        
        chemin_dossier_courant = os.path.join(chemin_dossier, f'M0{i}')  # Chemin du répertoire actuel
        
        if not os.path.exists(chemin_dossier_courant):  # si le répertoire n'existe pas
            print(f"Le répertoire {chemin_dossier_courant} n'existe pas.")
            continue  # passer à la prochaine itération

        files = [f for f in os.listdir(chemin_dossier_courant) if f.endswith('.csv')]  # récupérer uniquement les fichiers .csv
        
        if not files:  # si la liste de fichiers est vide
            print(f"Le répertoire {chemin_dossier_courant} ne contient aucun fichier .csv.")
            continue  # passer à la prochaine itération
        
        df_final, stats_apres = traiter_dossier(chemin_dossier_courant, files, date, parquet, bague)  # Traiter le dossier
        
        end_time_file = time.time()  # Mesurer le temps d'arrêt pour ce fichier
        
        execution_time_file = (end_time_file - start_time_file)  # Calculer la durée totale pour ce fichier
        
        print(f"Temps d'exécution pour {chemin_dossier_courant} : {execution_time_file:.2f} secondes")
        
        results.append((df_final, stats_apres))
        
    
    end_time_total = time.time()
    execution_time_total = (end_time_total - start_time_total)  # Calculer la durée totale en millisecondes

    print(f"\nTemps d'exécution total : {execution_time_total:.2f} secondes")
    
    return results

def traiter_dossier(root, files, date, parquets, bague):
    """
    Traite un dossier contenant des fichiers CSV et renvoie le DataFrame final résultant de la concaténation.

    Args:
        root (str): Chemin du répertoire à traiter.
        files (list): Liste des fichiers CSV à traiter.
        date (int): Timestamp numérique de la date à utiliser pour les calculs.
        parquets (dict): Dictionnaire de correspondance entre 'mangeoire' et 'parquet'.

    Returns:
        df_final (pd.DataFrame): DataFrame final résultant de la concaténation des DataFrames individuels.
        stats_final (list): Liste de dictionnaires contenant les statistiques après traitement pour chaque fichier.
    """
    
    # Initialiser une liste pour stocker les DataFrames
    dfs = []
    
    try:
        conn = sqlite3.connect('results_chunks.db')
        existing_file_name = pd.read_sql(f"SELECT Fichier FROM results", conn).values
    except:
        existing_file_name=[]
    
    for fichier in files:
        if fichier.endswith('.csv'):
            # Chemin complet vers le fichier
            chemin_fichier = os.path.join(root, fichier)
            
            if chemin_fichier not in existing_file_name:
                
                # Lecture du fichier CSV et ajout d'une colonne avec le nom du fichier
                df, stats_apres = traiter_fichier(chemin_fichier, date, parquets, bague)  # Traitement du fichier
            
                # Ajouter le DataFrame au liste des DataFrames
                dfs.append((df, stats_apres ))
            
            # gc.collect()  
            
    if not dfs:  # si la liste de DataFrames est vide
        return pd.DataFrame(), []  # retourner un DataFrame vide
    
    else:
        df_final = pd.concat([df for df, _ in dfs], ignore_index=True)  # concaténer les DataFrames

        stats_final = [stats for _, stats in dfs]
        
        # gc.collect()  
        
        return df_final, stats_final


def enregistrer_resultats(resultats, chemin_fichier):
    """
    Enregistre les résultats dans un fichier SQLite.

    Args:
        resultats (list): Liste de tuples où chaque tuple contient un DataFrame et ses statistiques.
        chemin_fichier (str): Chemin du fichier SQLite où enregistrer les résultats.

    Returns:
        None
    """

    # Créer une connexion au fichier SQLite
    conn = sqlite3.connect(chemin_fichier)

    try:
        # for df in resultats:
        # Créer une table 'results' dans le fichier SQLite et y insérer les données
        resultats.to_sql('results', con=conn, if_exists='append', index=False)
        
    except sqlite3.Error as e:
        print(f"Erreur lors de l'enregistrement des résultats : {e}")

    finally:
        # Fermer la connexion au fichier SQLite
        conn.close()


def recuperer_date_yaml(chemin_fichier):
    """
    Récupère une date à partir d'un fichier YAML.

    Args:
        chemin_fichier (str): Chemin du fichier YAML contenant la date.

    Returns:
        timestamp (int): Timestamp numérique de la date récupérée.
    """

    # Vérifier si le fichier YAML existe
    if not os.path.exists(chemin_fichier):
        # Créer un fichier YAML vide si celui-ci n'existe pas
        with open(chemin_fichier, 'w') as f:
            yaml.dump({'date': None}, f)

    try:
        with open(chemin_fichier, 'r') as f:
            data = yaml.safe_load(f)
            date_str = data['date']
            
            # Convertir la date en timestamp numérique
            if isinstance(date_str, str):
                return int(datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').timestamp())
            elif isinstance(date_str, datetime):
                return int(date_str.timestamp())  # Le timestamp est déjà un entier
    except FileNotFoundError:
        print("Le fichier YAML n'existe pas.")
        return None
    except KeyError:
        print("La clé 'date' n'est pas présente dans le fichier YAML.")
        return None
    
def recuperer_parquet_yaml(chemin_fichier):
    """
    Récupère une correspondance de mangeoires à partir d'un fichier YAML.
    
    Args:
        chemin_fichier (str): Chemin du fichier YAML contenant la liste des correspondances de mangeoires/parquet.
    
    Returns:
        dict: Dictionnaire contenant les correspondances de mangeoires/parquet.
    """

    try:
        with open(chemin_fichier, 'r') as f:
            data = yaml.safe_load(f)
            date_str = data['liste_mangeoires']
        return date_str
    except FileNotFoundError:
        print("Le fichier YAML n'existe pas.")
        return None
    except KeyError:
        print("La clé 'liste_mangeoires' n'est pas présente dans le fichier YAML.")
        return None

def recuperer_bague_yaml(chemin_fichier):
    """
    Récupère une bague à partir d'un fichier YAML.
    
    Args:
        chemin_fichier (str): Chemin du fichier YAML contenant la liste des bagues.
    
    Returns:
        dict: Dictionnaire contenant les informations de la bague.
    """

    try:
        with open(chemin_fichier, 'r') as f:
            data = yaml.safe_load(f)
            date_str = data['bague']
        return date_str
    except FileNotFoundError:
        print("Le fichier YAML n'existe pas.")
        return None
    except KeyError:
        print("La clé 'bague' n'est pas présente dans le fichier YAML.")
        return None
    
def init_database():
    """Initialise la base de données en supprimant les tables existantes."""
    conn = sqlite3.connect('results_chunks.db')
    cursor = conn.cursor()
    
    # Suppression des tables si elles existent déjà
    cursor.execute("DROP TABLE IF EXISTS aggregate_bague_source;")
    cursor.execute("DROP TABLE IF EXISTS conso_indiv_semaine_mangeoire;")
    cursor.execute("DROP TABLE IF EXISTS conso_indiv_semaine_parquet;")
    cursor.execute("DROP TABLE IF EXISTS bague_heure;")
    conn.commit()  # Sauvegarde des modifications
    
    conn.close()

def creer_table_aggregate_bague_source():
    """Crée la table 'aggregate_bague_source' si elle n'existe pas déjà."""
    conn = sqlite3.connect('results_chunks.db')
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

def creer_table_conso_indiv_semaine_mangeoire():
    """Crée la table 'utilisation_mangeoires' si elle n'existe pas déjà."""
    conn = sqlite3.connect('results_chunks.db')
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
CREATE TABLE conso_indiv_semaine_mangeoire AS
SELECT
	mangeoire,
	evenement,
	bague,
	sources_concatenées,
	jour, 
	sum(conso_g) as quantite_grammes	
FROM
	(SELECT 
	    r.*, 
	    abs.bague,
	    abs.sources_concatenées
	FROM 
	    results r
	INNER JOIN 
	    aggregate_bague_source abs ON r.bague = abs.bague)
GROUP BY bague, jour, mangeoire, sources_concatenées, evenement;
""")
        conn.commit()  # Sauvegarde des modifications
    except sqlite3.OperationalError as e:
        if "already exists" in str(e):
            print("La table utilisation_mangeoires existe déjà.")
        else:
            raise
    
    conn.close()
    
def creer_table_conso_indiv_semaine_parquet():
    """Crée la table 'utilisation_mangeoires' si elle n'existe pas déjà."""
    conn = sqlite3.connect('results_chunks.db')
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
CREATE TABLE conso_indiv_semaine_parquet AS
SELECT
	parquet,
	evenement,
	bague,
	sources_concatenées,
	jour, 
	sum(conso_g) as quantite_grammes	
FROM
	(SELECT 
	    r.*, 
	    abs.bague,
	    abs.sources_concatenées
	FROM 
	    results r
	INNER JOIN 
	    aggregate_bague_source abs ON r.bague = abs.bague)
GROUP BY bague, jour, parquet, sources_concatenées, evenement;
""")
        conn.commit()  # Sauvegarde des modifications
    except sqlite3.OperationalError as e:
        if "already exists" in str(e):
            print("La table utilisation_parquet existe déjà.")
        else:
            raise
    
    conn.close()
    
def creer_table_bague_heure():
    """Crée la table 'utilisation_mangeoires' si elle n'existe pas déjà."""
    conn = sqlite3.connect('results_chunks.db')
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
    
def creer_vue_heure(colonne):
    """Crée une vue pour une colonne spécifique."""
    conn = sqlite3.connect('results_chunks.db')
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
    
def creer_vue_heure_parquet(colonne):
    """Crée une vue pour une colonne spécifique."""
    conn = sqlite3.connect('results_chunks.db')
    cursor = conn.cursor()
    
    try:
        query_parts = [
            f"SUM(CASE WHEN heure = {h} THEN {colonne} END) AS H_{h:02d}_{colonne}"
            for h in range(24)
        ]
        
        query = f"""
CREATE VIEW IF NOT EXISTS vue_heure_parquet_{colonne} AS
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
    parquet, evenement, bague, jour;
"""
        
        cursor.execute(query)
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"Erreur lors de la création de la vue 'vue_mangeoire_heure_{colonne}' : {e}")
    
    conn.close()

def creer_vue_jour(colonne):
    """Crée une vue pour une colonne spécifique."""
    conn = sqlite3.connect('results_chunks.db')
    cursor = conn.cursor()
    
    try:
        """Crée une vue pour une colonne spécifique."""
        conn = sqlite3.connect('results_chunks.db')
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
    
def creer_vue_jour_parquet(colonne):
    """Crée une vue pour une colonne spécifique."""
    conn = sqlite3.connect('results_chunks.db')
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
        
        print (query_parts_2)
        
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
        print(query)
        
        cursor.execute(query)
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"Erreur lors de la création de la vue 'vue_mangeoire_heure_{colonne}' : {e}")
    
    conn.close()
    

def creer_vue_jour_parquet_(colonne):
    """Crée une vue pour une colonne spécifique."""
    
    # Connexion à la base de données
    conn = sqlite3.connect('results_chunks.db')
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

def creer_vue_semaine(colonne):
    """Crée une vue pour une colonne spécifique."""
    conn = sqlite3.connect('results_chunks.db')
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
    
def creer_vue_semaine_parquet(colonne):
    """Crée une vue pour une colonne spécifique."""
    conn = sqlite3.connect('results_chunks.db')
    cursor = conn.cursor()
    
    try:
        # Récupération du maximum de la colonne 'semaine'
        cursor.execute("SELECT MAX(semaine) FROM bague_heure")
        max_semaine = cursor.fetchone()[0]
       
        if max_semaine is None:
           print("La table 'bague_heure' est vide.")
           return
       
        query_parts = [
            f"SUM(CASE WHEN jour = {s+1} THEN {colonne} END) AS s_{(s+1):02d}_{colonne}"
            for s in range(max_semaine)
        ]
        
        query = f"""
CREATE VIEW IF NOT EXISTS vue_semaine_parquet_{colonne} AS
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
    parquet, evenement, bague;
"""
        
        cursor.execute(query)
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"Erreur lors de la création de la vue 'vue_mangeoire_heure_{colonne}' : {e}")
    
    conn.close()
    
def database_construct():
    """Point d'entrée du programme."""
    init_database()
    creer_table_aggregate_bague_source()
    creer_table_conso_indiv_semaine_mangeoire()
    creer_table_conso_indiv_semaine_parquet()
    creer_table_bague_heure()
    
    # Création des vues génériques
    # creer_vue_heure('quantite_grammes')
    creer_vue_heure('nombre_prises')
    # creer_vue_heure('duree_prises')
    
    # creer_vue_heure_parquet('quantite_grammes')
    creer_vue_heure_parquet('nombre_prises')
    # creer_vue_heure_parquet('duree_prises')
    
    # creer_vue_jour('quantite_grammes')
    creer_vue_jour('nombre_prises')
    # creer_vue_jour('duree_prises')
    
    # creer_vue_jour_parquet('quantite_grammes')
    creer_vue_jour_parquet_('nombre_prises')
    # creer_vue_jour_parquet('duree_prises')
    
    # creer_vue_semaine('quantite_grammes')
    creer_vue_semaine('nombre_prises')
    # creer_vue_semaine('duree_prises')
    
    # creer_vue_semaine_parquet('quantite_grammes')
    creer_vue_semaine_parquet('nombre_prises')
    # creer_vue_semaine_parquet('duree_prises')
    
if __name__ == "__main__":
    import yaml
    resultats = main()
    database_construct()
