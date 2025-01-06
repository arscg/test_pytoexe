# -*- coding: utf-8 -*-
"""
Created on Mon Jan  6 14:11:51 2025

@author: Utilisateur
"""

import pandas as pd
from datetime import datetime
import sqlite3
import os
import time
import gc
import utils
import config

def convertir_date_locale_en_timestamp(df):
    df['timestamp'] = pd.to_datetime(df['date_locale'], dayfirst=True)
    df['timestamp_num'] = df['timestamp'].apply(lambda x: int(x.timestamp()))
    return df

def traiter_fichier(database_path, chemin_fichier, date, parquets, bague):
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
    
    try:
        conn = sqlite3.connect('results_originaux.db')
        existing_file_name = pd.read_sql(f"SELECT Fichier FROM results", conn).values
    except:
        existing_file_name=[]
        
    # Mesurer le temps d'exécution pour ce fichier
    start_time_file = datetime.now().timestamp()  
    
    if chemin_fichier not in existing_file_name:
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
        
        # Ajouter la colonne compt avec le nombre de lignes par groupe
        result_df['compt'] = grouped_df.size().reset_index(name='nombre_lignes')['nombre_lignes']
    
        # Calculer les statistiques après traitement
        stats_apres = utils.calculer_stats(df, chemin_fichier )
        
        # Enregistrer le résultat dans une base de données SQLite
        utils.enregistrer_resultats(result_df, database_path)
        
        end_time_file = time.time() 
        
        execution_time_file = (end_time_file - start_time_file)
        
        print(f"Temps d'exécution pour {chemin_fichier} : {execution_time_file:.2f} secondes")
        
        return result_df, stats_apres
    
    end_time_file = time.time() 
    
    execution_time_file = (end_time_file - start_time_file)
    
    print(f"Temps d'exécution pour {chemin_fichier} : {execution_time_file:.2f} secondes")
    
    return None

def traiter_tous_les_fichiers(database_path, chemin_dossier, date, parquets, bague):
    """
    Traite tous les fichiers CSV dans un dossier et renvoie le DataFrame final résultant de la concaténation.

    Args:
        chemin_dossier (str): Chemin du dossier contenant les fichiers CSV à traiter.

    Returns:
        df_final (pd.DataFrame): DataFrame final résultant de la concaténation des DataFrames individuels.
    """
    
    # Initialiser une liste pour stocker les DataFrames
    dfs = []
    
    for fichier in os.listdir(chemin_dossier):
        if fichier.endswith('.csv'):
            # Chemin complet vers le fichier
            chemin_fichier = os.path.join(chemin_dossier, fichier)
            
            # Lecture du fichier CSV et ajout d'une colonne avec le nom du fichier
            df = traiter_fichier(database_path, chemin_fichier, date, parquets, bague)  
            # df['Fichier'] = fichier  # Ajout d'une colonne avec le nom du fichier
            
            # Ajouter le DataFrame au liste des DataFrames
            dfs.append(df)
            
            # Collecte des déchets après chaque traitement de fichier
            gc.collect()  
    
    if not dfs:  # si la liste de DataFrames est vide
        return pd.DataFrame()  # retourner un DataFrame vide
    
    else:
        return  pd.DataFrame() # pd.concat(dfs, ignore_index=True)  # concaténer les DataFrames

