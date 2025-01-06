# -*- coding: utf-8 -*-
"""
Created on Mon Jan  6 14:09:25 2025

@author: Utilisateur
"""

import pandas as pd
from datetime import datetime
import os
import sqlite3

def calculer_stats_numeriques(df):
    stats = df[['avant_g', 'apres_g', 'conso_g', 'duree_s']].describe()
    return {'numerique': stats}

def calculer_stats_non_numeriques_par_colonne(df):
    stats = {}
    for colonne in df.columns:
        if not pd.api.types.is_numeric_dtype(df[colonne]):
            stats[colonne] = {
                'value_counts': df[colonne].value_counts(),
                'unique': df[colonne].nunique(),
                'null': df[colonne].isnull().sum()
            }
    return {'non_numerique_par_colonne': stats}

def calculer_stats(df, chemin_fichier):
    stats_numerique = calculer_stats_numeriques(df)
    stats_non_numerique_par_colonne = calculer_stats_non_numeriques_par_colonne(df)
    return {'numerique': stats_numerique, 'non_numerique': stats_non_numerique_par_colonne, 'fichier': chemin_fichier }

def convertir_date_locale_en_timestamp(df):
    df['timestamp'] = pd.to_datetime(df['date_locale'], dayfirst=True)
    df['timestamp_num'] = df['timestamp'].apply(lambda x: int(x.timestamp()))
    return df

def enregistrer_resultats(resultats, database):
    conn = sqlite3.connect(database)
    try:
        resultats.to_sql('results', con=conn, if_exists='append', index=False)
    except sqlite3.Error as e:
        print(f"Erreur lors de l'enregistrement des résultats : {e}")
    finally:
        conn.close()
