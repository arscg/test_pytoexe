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
    """
    Fonction principale qui traite tous les fichiers CSV dans le dossier spécifié.
    """
    
    start_time_total = time.time()
    
    database_path = config.recuperer_database_yaml('config.yaml')
    dataset_path = config.recuperer_dataset_yaml('config.yaml')
    date = config.recuperer_date_yaml('config.yaml')
    parquet = config.recuperer_parquet_yaml('config.yaml')
    bague = config.recuperer_bague_yaml('config.yaml')
    
    
    results = csv_processing.traiter_tous_les_fichiers(database_path, dataset_path, date, parquet, bague)  # Traiter tous les fichiers CSV
    
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
