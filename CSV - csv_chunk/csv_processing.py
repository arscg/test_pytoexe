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

def traiter_fichier(chemin_fichier, database, date, parquets, bague):
    start_time_file = time.time()  
    
    df = pd.read_csv(chemin_fichier, sep=',')
    
    df = utils.convertir_date_locale_en_timestamp(df)
    
    df['Fichier'] = chemin_fichier
   
    df['parquet'] = df['mangeoire'].map(parquets)
    df['bague'] = df['source'].map(bague)
    
    df['jour'] = (pd.to_timedelta(df['timestamp_num'] - date, unit='s').dt.days) + 1
    df['semaine'] = ((pd.to_timedelta(df['timestamp_num'] - date, unit='s').dt.days)//7) + 1
    
    df = df.drop(columns=['date_locale', 'timestamp', 'timestamp_num'])
    
    df['heure'] = df['heures'].str.split(':').str[0]
    
    grouped_df = df.groupby(['date', 'mangeoire', 'evenement', 'source', 'bague', 'Fichier', 'parquet', 'heure',  'jour', 'semaine'])
    result_df = grouped_df[['avant_g', 'apres_g', 'conso_g', 'duree_s']].sum().reset_index()
    result_df['compt'] = grouped_df.size().reset_index(name='nombre_lignes')['nombre_lignes']

    stats_apres = utils.calculer_stats(df, chemin_fichier )
    
    utils.enregistrer_resultats(result_df, database)
    
    end_time_file = time.time()
    execution_time_file = end_time_file - start_time_file
    
    return result_df, stats_apres

def traiter_dossier(database, root, files, date, parquets, bague):
    dfs = []
    try:
        conn = sqlite3.connect(database)
        existing_file_name = pd.read_sql(f"SELECT Fichier FROM results", conn).values
    except:
        existing_file_name=[]
    
    for fichier in files:
        if fichier.endswith('.csv'):
            chemin_fichier = os.path.join(root, fichier)
            if chemin_fichier not in existing_file_name:
                df, stats_apres = traiter_fichier(chemin_fichier, database, date, parquets, bague)
                dfs.append((df, stats_apres ))
    
    if not dfs:  
        return pd.DataFrame(), []
    
    else:
        df_final = pd.concat([df for df, _ in dfs], ignore_index=True)  
        stats_final = [stats for _, stats in dfs]
        return df_final, stats_final
