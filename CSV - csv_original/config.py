# -*- coding: utf-8 -*-
"""
Created on Mon Jan  6 14:11:09 2025

@author: Utilisateur
"""

import os
import yaml
import pandas as pd
import sqlite3
from datetime import datetime

def recuperer_date_yaml(chemin_fichier):
    if not os.path.exists(chemin_fichier):
        with open(chemin_fichier, 'w') as f:
            yaml.dump({'date': None}, f)

    try:
        with open(chemin_fichier, 'r') as f:
            data = yaml.safe_load(f)
            date_str = data['date']
            if isinstance(date_str, str):
                return int(datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').timestamp())
            elif isinstance(date_str, datetime):
                return int(date_str.timestamp())
    except FileNotFoundError:
        print("Le fichier YAML n'existe pas.")
        return None
    except KeyError:
        print("La clé 'date' n'est pas présente dans le fichier YAML.")
        return None
    
def recuperer_parquet_yaml(chemin_fichier):
    try:
        with open(chemin_fichier, 'r') as f:
            data = yaml.safe_load(f)
            return data['liste_mangeoires']
    except FileNotFoundError:
        print("Le fichier YAML n'existe pas.")
        return None
    except KeyError:
        print("La clé 'liste_mangeoires' n'est pas présente dans le fichier YAML.")
        return None

def recuperer_bague_yaml(chemin_fichier):
    try:
        with open(chemin_fichier, 'r') as f:
            data = yaml.safe_load(f)
            return data['bague']
    except FileNotFoundError:
        print("Le fichier YAML n'existe pas.")
        return None
    except KeyError:
        print("La clé 'bague' n'est pas présente dans le fichier YAML.")
        return None

def recuperer_dataset_yaml(chemin_fichier):
    try:
        with open(chemin_fichier, 'r') as f:
            data = yaml.safe_load(f)
            return data['dataset_path']
    except FileNotFoundError:
        print("Le fichier YAML n'existe pas.")
        return None
    except KeyError:
        print("La clé 'dataset_path' n'est pas présente dans le fichier YAML.")
        return None
    
def recuperer_database_yaml(chemin_fichier):
    try:
        with open(chemin_fichier, 'r') as f:
            data = yaml.safe_load(f)
            return data['database_path']
    except FileNotFoundError:
        print("Le fichier YAML n'existe pas.")
        return None
    except KeyError:
        print("La clé 'database_path' n'est pas présente dans le fichier YAML.")
        return None
