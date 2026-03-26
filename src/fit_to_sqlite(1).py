from fitparse import FitFile
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pathlib import Path
import sqlite3


#--- SETUP SQLITE STRUCTURE ----
conn = sqlite3.connect('alex_overview.sqlite')
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
        max_heart_rate INTEGER
        )
''')

#---- Fetch all fitfiles
kolommen = [
    'start_time', 'avg_temperature', 'unknown_110', 'total_distance', 
    'total_ascent', 'avg_speed', 'avg_power', 'normalized_power', 
    'max_power', 'threshold_power', 'avg_cadence', 'avg_heart_rate', 'max_heart_rate'
]

base_path = Path("files")
for file_path in base_path.rglob("*"):
    if file_path.is_file():
        try:
            print(f"----Bezig met bestand: {file_path.name} ----")
            
            fit_file = FitFile(str(file_path))
            full_data={}
            for message_ses in fit_file.get_messages('session'):
                full_data.update({f.name: f.value for f in message_ses.fields if f.name in kolommen})
            for message_file_id in fit_file.get_messages('file_id'):
                full_data.update({f.name: f.value for f in message_file_id.fields})
            
            placeholders = ", ".join(["?"] * len(kolommen))
            kolom_namen = ", ".join(kolommen)
            
            sql = f"INSERT OR IGNORE INTO Ritten ({kolom_namen}) VALUES({placeholders})"
           
            data_tuple = tuple(full_data.get(k) for k in kolommen)
            
            cur.execute(sql, data_tuple)
            
        except Exception as e:
            print(f"Error parsing: {file_path.name}: {e}")

conn.commit()
conn.close()
