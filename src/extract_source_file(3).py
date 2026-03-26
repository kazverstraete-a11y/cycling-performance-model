import pandas as pd
import glob
import sqlite3
from tqdm import tqdm

conn = sqlite3.connect('alex_overview.sqlite')
cur = conn.cursor()

parquet_files = glob.glob('files/parquet_files/*.parquet')
mapping_list = []

for f in tqdm(parquet_files, desc='Extracting all source_files...):
    df = pd.read_parquet(f, columns=['timestamp', 'source_file'])
    df['timestamp'] = df['timestamp'].astype(str)
    df[['datum', 'startuur']] = df['timestamp'].str.split(' ', expand=True)
    df = df.drop(columns=['timestamp'])
    df['startuur']=df['startuur'].str.split('+').str[0]
    
    mapping_list.append(df.iloc[0].to_dict())

df_mapping = pd.DataFrame(mapping_list)

# Write to temporary SQL table
df_mapping.to_sql('temp_mapping', conn, if_exists='replace', index=False)

# Link with existing 'Ritten' table
cur.execute('''
    UPDATE Ritten
    SET source_file = (
        SELECT m.source_file
        FROM temp_mapping m
        WHERE m.datum = Ritten.datum
            AND m.startuur = Ritten.startuur
    )
    WHERE EXISTS(
        SELECT 1 FROM temp_mapping m
        WHERE m.datum = Ritten.datum
            AND m.startuur = Ritten.startuur
    );
''')

conn.commit()
conn.close()

print("Mapping completed: source_file successfully linked to sessions.")
