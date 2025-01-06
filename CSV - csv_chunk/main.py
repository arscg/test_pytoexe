# -*- coding: utf-8 -*-
"""
Created on Mon Jan  6 14:09:38 2025

@author: Utilisateur
"""

import os
import time
import gc
import yaml
import utils
import csv_processing
import database
import config

def main():
    start_time_total = time.time()
    
    database_path = config.recuperer_database_yaml('config.yaml')
    dataset_path = config.recuperer_dataset_yaml('config.yaml')
    date = config.recuperer_date_yaml('config.yaml')
    parquet = config.recuperer_parquet_yaml('config.yaml')
    bague = config.recuperer_bague_yaml('config.yaml')

    results = []
    
    for i in range(1, 5):
        start_time_file = time.time()
        
        chemin_dossier_courant = os.path.join(dataset_path, f'M0{i}')
        
        if not os.path.exists(chemin_dossier_courant):
            print(f"Le répertoire {chemin_dossier_courant} n'existe pas.")
            continue

        files = [f for f in os.listdir(chemin_dossier_courant) if f.endswith('.csv')]
        
        if not files:
            print(f"Le répertoire {chemin_dossier_courant} ne contient aucun fichier .csv.")
            continue
        
        df_final, stats_apres = csv_processing.traiter_dossier(database_path, chemin_dossier_courant, files, date, parquet, bague)
        
        end_time_file = time.time()
        execution_time_file = end_time_file - start_time_file
        
        print(f"Temps d'exécution pour {chemin_dossier_courant} : {execution_time_file:.2f} secondes")
        
        results.append((df_final, stats_apres))
    
    end_time_total = time.time()
    execution_time_total = end_time_total - start_time_total

    print(f"\nTemps d'exécution total : {execution_time_total:.2f} secondes")

    database.database_construct(database_path)
    
    end_time_total = time.time()
    execution_time_total = end_time_total - start_time_total

    print(f"\nTemps d'exécution total aprés construction de la base de données: {execution_time_total:.2f} secondes")
    
    return results

if __name__ == "__main__":
    resultats = main()
