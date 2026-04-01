import numpy as np
import pandas as pd
import sqlite3

from pathlib import Path
from tqdm import tqdm
from fitparse import FitFile

# -------------------------------------------------------
# --------------------- SQL DB SETUP --------------------
# -------------------------------------------------------

conn = sqlite3.connect('overview.sqlite')
cur = conn.cursor()

cur.executescript('''
    DROP TABLE IF EXISTS Ritten;
    CREATE TABLE Ritten(
        id INTEGER PRIMARY KEY,
        start_time DATETIME,
        avg_temperature INTEGER,
        unknown_110 TEXT, 
        total_distance FLOAT,
        total_ascent INTEGER,
        avg_speed FLOAT,
        avg_power INTEGER,
        normalized_power INTEGER,
        max_power INTEGER,
        threshold_power INTEGER,
        avg_cadence INTEGER, 
        avg_heart_rate INTEGER,
        max_heart_rate INTEGER,
        manufacturer TEXT,
        unknown_0 FLOAT,
        unknown_1 INT, 
        unknown_11 INT,
        source_file TEXT
        )
''')

# ---------------------------------------------------------
# ----------------------- FIT TO SQL ----------------------
# ---------------------------------------------------------

kolommen = [
    'start_time', 'avg_temperature', 'unknown_110', 'total_distance', 
    'total_ascent', 'avg_speed', 'avg_power', 'normalized_power', 
    'max_power', 'threshold_power', 'avg_cadence', 'avg_heart_rate', 'max_heart_rate',
    'manufacturer', 'unknown_0', 'unknown_1', 'unknown_11', 'source_file']

base_path = Path("files")
placeholders = ", ".join(["?"] * len(kolommen))
kolom_namen = ", ".join(kolommen)
data_to_insert = []
sql_insert = f"INSERT OR IGNORE INTO Ritten ({kolom_namen}) VALUES({placeholders})"

# ---------------------------------------------------------
files = list(base_path.rglob("*"))
for file_path in tqdm(files, desc='Status: '):
    if file_path.is_file():
        try:    
            fit_file = FitFile(str(file_path))
            
            full_data={}
            full_data['source_file'] = file_path.name
            
            for message_ses in fit_file.get_messages('session'):
                temp_data = {f.name: f.value for f in message_ses.fields}
                full_data.update(temp_data)
        
                full_data['avg_speed'] = temp_data.get('avg_speed') or temp_data.get('enhanced_avg_speed')
                full_data['total_ascent'] = temp_data.get('enhanced_total_ascent') or temp_data.get('total_ascent')
                
            for message_file_id in fit_file.get_messages('file_id'):
                full_data.update({f.name: f.value for f in message_file_id.fields})
                
            for message_79 in fit_file.get_messages('unknown_79'):
                full_data.update({f.name: f.value for f in message_79.fields})
           
            data_tuple = tuple(full_data.get(k) for k in kolommen)
            data_to_insert.append(data_tuple)
            
        except Exception as e:
            print(f"Error parsing: {file_path.name}: {e}")

# ---------------------------------------------------------
if data_to_insert:
    cur.executemany(sql_insert, data_to_insert)
    conn.commit()
# ---------------------------------------------------------
conn.close()
print("\nKlaar! Alles staat nu in de database met de juiste bronbestanden.")
