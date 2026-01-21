import pandas as pd

# Charger le fichier CSV
df = pd.read_csv('data_processed_final.csv')

# Convertir la colonne 'datetime' en objet datetime pour un tri précis
df['datetime'] = pd.to_datetime(df['datetime'])

# Trier le DataFrame par la colonne 'datetime' (ordre ascendant par défaut)
df_sorted = df.sort_values(by='datetime')

# Enregistrer le fichier trié dans un nouveau CSV (sans index supplémentaire)
df_sorted.to_csv('data_processed_final_sorted.csv', index=False)

print("Le fichier a été trié et enregistré sous 'data_processed_final_sorted.csv'.")