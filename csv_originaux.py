# -*- coding: utf-8 -*-
"""
Created on Thu May 23 09:14:59 2024

@author: dsite
"""

import pandas as pd
import os
import time
import gc

def traiter_fichier(chemin_fichier):
    """
    Traite un fichier CSV et renvoie le DataFrame correspondant.

    Args:
        chemin_fichier (str): Chemin du fichier CSV à traiter.

    Returns:
        df (pd.DataFrame): DataFrame résultant de la lecture du fichier CSV.
    """
    
    # Mesurer le temps d'exécution pour ce fichier
    start_time_file = time.time()  
    
    # Lecture du fichier CSV en utilisant la séparation virgulee
    df = pd.read_csv(chemin_fichier, sep=',')
    
    # Mesurer le temps d'arrêt pour ce fichier
    end_time_file = time.time()  
    
    # Calculer la durée totale en millisecondes pour ce fichier
    execution_time_file = (end_time_file - start_time_file) * 1000  
    
    # Afficher le temps d'exécution pour ce fichier
    print(f"Temps d'exécution pour {chemin_fichier} : {execution_time_file:.2f} millisecondes")
    
    return df

def traiter_tous_les_fichiers(chemin_dossier):
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
            df = traiter_fichier(chemin_fichier)  
            df['Fichier'] = fichier  # Ajout d'une colonne avec le nom du fichier
            
            # Ajouter le DataFrame au liste des DataFrames
            dfs.append(df)
            
            # Collecte des déchets après chaque traitement de fichier
            gc.collect()  
    
    if not dfs:  # si la liste de DataFrames est vide
        return pd.DataFrame()  # retourner un DataFrame vide
    
    else:
        return pd.concat(dfs, ignore_index=True)  # concaténer les DataFrames

def main():
    """
    Fonction principale qui traite tous les fichiers CSV dans le dossier spécifié.
    """
    
    chemin_dossier = r'.\CSV\originaux'  # Chemin du dossier contenant les fichiers CSV
    
    df_final = traiter_tous_les_fichiers(chemin_dossier)  # Traiter tous les fichiers CSV
    
    start_time_total = time.time()  # Mesurer le temps d'exécution total
    
    end_time_total = time.time()
    
    execution_time_total = end_time_total - start_time_total
    
    print(f"Temps d'exécution total : {execution_time_total:.2f} secondes")
    
    return df_final

if __name__ == "__main__":
    df = main()  # Appeler la fonction principale
