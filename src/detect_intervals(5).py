#-------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------
#----------------------- !!!! NEEDS TO BE EXECUTED IN MAMBA !!!!!!  ------------------------
#-------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------
# terminal: bash conda activate fit_env

import pandas as pd
import os
import sqlite3

#------------------------------------------------------------------------------------------
#-------------------------------------- FUNCTIONS(+ Mappings) -----------------------------
#------------------------------------------------------------------------------------------
def file_ftp_fetcher(db_path):
    #power files
    conn = sqlite3.connect(db_path)
    query_power = (""" SELECT threshold_power, source_file FROM Ritten 
                WHERE activity_type = 'WIELRENNEN' AND avg_power > 0
                """)
    df_power = pd.read_sql(query_power, conn)
    power_files = [os.path.splitext(f)[0] + '.parquet' for f in df_power['source_file']]
    conn.close()
    
    ftp_dict = {}
    for _, row in df_power.iterrows():
        name_only = os.path.splitext(row['source_file'])[0]
        ftp_dict[name_only] = row['threshold_power']
    
    return power_files, ftp_dict

def get_np(power_series):
    if len(power_series) < 30:
        return power_series.mean()
    
    moving_avg = power_series.rolling(window=30, min_periods=1).mean()
    return (moving_avg**4).mean()**0.25

def calc_decoup(interval):
    half = len(interval) // 2
    
    if half < 30: return 0.0
    
    h1, h2 = interval.iloc[:half], interval.iloc[half:]
    
    hr1_mean = h1['heart_rate'].mean()
    hr2_mean = h2['heart_rate'].mean()
    
    if hr1_mean == 0 or hr2_mean == 0 or pd.isna(hr1_mean) or pd.isna(hr2_mean):
        return 0.0
    
    ef1 = get_np(h1['power']) / h1['heart_rate'].mean()
    ef2 = get_np(h2['power']) / h2['heart_rate'].mean()
    
    if ef1 == 0: return 0.0
    
    decoupling = ((ef1 - ef2) / ef1) * 100
    
    return decoupling
    
def detect_intervals(df):
    # ---- Micro-coast buffer ----
    df['temp_id'] = df['is_werkblok'].diff().ne(0).cumsum()
    block_sizes = df.groupby('temp_id').size()
    block_status = df.groupby('temp_id')['is_werkblok'].first()
    
    short_rest = (block_status == 0) & (block_sizes <= 15)
    
    df.loc[df['temp_id'].isin(short_rest[short_rest].index), 'is_werkblok'] = 1
    
    # ---- After buffer detector ----
    df['interval_id'] = df['is_werkblok'].diff().ne(0).cumsum()
    
    all_blocks = df.groupby('interval_id').agg(
        status = ('is_werkblok', 'first'),
        duur = ('is_werkblok', 'count'),
        cadence = ('cadence', 'mean'),
        avg_hr = ('heart_rate', 'mean'),
        avg_power = ('power', 'mean'),
        starttijd = ('timestamp', 'first')
    )
    
    all_blocks['rest'] = all_blocks['duur'].shift(-1)
    
    custom_metrics = df.groupby('interval_id').apply(lambda x: pd.Series({
        'np_power': get_np(x['power']),
        'decoupling': calc_decoup(x),
    }))
    
    all_intervals = pd.concat([all_blocks, custom_metrics], axis=1)
    all_intervals['EF'] = all_intervals['np_power'] / all_intervals['avg_hr']
    all_intervals['source_file'] = df['source_file'].iloc[0]
    all_intervals['rest'] = all_intervals['rest'].fillna(0)
    
    return all_intervals

def intervals_mapper(row, ftp):
    intensity = row['avg_power'] / ftp
    duur = row['duur']
    rust = row['rest']
    cadence = row['cadence']
    
    for label, rules in interval_mapping.items():
        
        f_min, f_max = rules.get('watts_range', (0,999))
        d_min, d_max = rules.get('duration_range', (0,99999))
        r_min, r_max = rules.get('rest_range', (0,99999))
        c_min, c_max = rules.get('cadence_range', (0,250))
        
        if (f_min <= intensity <= f_max and
            d_min <= duur <= d_max and
            r_min <= rust <= r_max and
            c_min <= cadence <= c_max):
            
            return label
    
    if intensity > 1.20: return "Anaerobic (Unclassified)"
    if 1.06 <= intensity <= 1.20: return "VO2 Max (Unclassified)"
    if 0.88 <= intensity <= 1.05: return "Threshold/SS (Unclassified)"
    if 0.76 <= intensity <= 0.87: return "Tempo (Unclassified)"
    if 0.55 <= intensity <= 0.75999: return "Z2 (Unclassified)"
    
    return "Z1/Coast (Unclassified)"    

interval_mapping = {
    # ==========================================
    # 1. SPECIFIC (Strict rest / cadence)
    # ==========================================
    "Tabata_Micro": {
        "watts_range": (1.20, 1.70),
        "duration_range": (15, 45),      
        "rest_range": (10, 40),          
        "cadence_range": (73, 250)       
    },
    "VO2_Micro_Intervals": {             
        "watts_range": (1.06, 1.19999),  
        "duration_range": (15, 60),      
        "rest_range": (10, 60),          
        "cadence_range": (70, 250)       
    },
    "Strength_Low_Cadence": {
        "watts_range": (0.80, 1.20),
        "duration_range": (60, 1200),    
        "rest_range": (60, 99999),
        "cadence_range": (0, 72)         
    },
    "Long_climbs": {
        "watts_range": (0.82, 0.94999),
        "duration_range": (1000, 10800),
        "rest_range": (30, 99999), 
        "cadence_range": (0, 88)
    },
    # ==========================================
    # 2. NEUROMUSCULAR & ANAEROBIC
    # ==========================================
    "Sprint_Interval": {
        "watts_range": (1.71, 10.0),       
        "duration_range": (4, 12),      
        "rest_range": (0, 99999),        
        "cadence_range": (0, 250)
    },
    "Anaerobic_Capacity": {
        "watts_range": (1.21, 3.0),
        "duration_range": (13, 300),         
        "rest_range": (45, 99999),         
        "cadence_range": (0, 250)
    },
    # ==========================================
    # 3. MAIN ZONES (Macro Intervals)
    # ==========================================
    "VO2_Max_Short": {
        "watts_range": (1.06, 1.20999),
        "duration_range": (61, 179),     
        "rest_range": (0, 99999),
        "cadence_range": (73, 250)
    },
    "VO2_Max_Long": {
        "watts_range": (1.06, 1.20),
        "duration_range": (180, 480),   
        "rest_range": (0, 99999),
        "cadence_range": (73, 250)
    },
    "Threshold": {
        "watts_range": (0.95, 1.05999),
        "duration_range": (120, 3600),   
        "rest_range": (0, 99999),         
        "cadence_range": (0, 250)       # Ook hier verlaagd voor de zekerheid
    },
    "Sweet_Spot": {
        "watts_range": (0.85, 0.94999),     
        "duration_range": (120, 7200),   
        "rest_range": (0, 99999),         
        "cadence_range": (0, 250)       # Vrij baan voor de stoempers!
    },
    "Tempo": {
        "watts_range": (0.76, 0.84999),
        "duration_range": (120, 7200),   
        "rest_range": (0, 99999),        # Geen limiet meer op de rust!
        "cadence_range": (0, 250)       
    },
    # ==========================================
    # 4. ENDURANCE / BASE (Z2 - Z1)
    # ==========================================
    "Z2_Decoupling_Block": {
        "watts_range": (0.55, 0.75999),     # Jouw Z2 range
        "duration_range": (2400, 99999), # Vanaf 40 minuten!
        "rest_range": (0, 99999),
        "cadence_range": (60, 250)
    },
    "Z2_Endurance": {
        "watts_range": (0.55, 0.75999),
        "duration_range": (0, 2399),     # Korter dan 40 minuten
        "rest_range": (0, 99999),
        "cadence_range": (60, 250)
    },
    # ==========================================
    # 5.SURGE CATEGORIES (PHYSIOLOGIC BANTER)
    # ==========================================
    "Anaerobic_Surge": {
        "watts_range": (1.21, 3.0),
        "duration_range": (0, 300),      
        "rest_range": (0, 99999),           
        "cadence_range": (0, 250)
    },
    "VO2_Surge": {
        "watts_range": (1.06, 1.20999),
        "duration_range": (0, 119),     
        "rest_range": (0, 99999),       #
        "cadence_range": (0, 250)
    },
    "Threshold_Surge": {
        "watts_range": (0.95, 1.05999),
        "duration_range": (0, 119),     
        "rest_range": (0, 99999),         
        "cadence_range": (0, 250)       
    },
    "Sweet_Spot_Surge": {
        "watts_range": (0.85, 0.94999),  
        "duration_range": (0, 119),   # AANGEPAST
        "rest_range": (0, 99999),        
        "cadence_range": (0, 250)     
    },
    "Tempo_Surge": {
        "watts_range": (0.76, 0.84999),
        "duration_range": (0, 119),     # AANGEPAST
        "rest_range": (0, 99999),
        "cadence_range": (0, 250)       
    }
}
# ------------------------------------------------------------------------------------------
# ------------------------------------- MAIN LOOP -----------------------------------------
# ------------------------------------------------------------------------------------------

#--- INITIATE ---
db_path = 'overview.sqlite'
parquet_folder = 'parquet_files'

files, ftp_dict = file_ftp_fetcher(db_path)

#--- UPDATE POWER FILES --- 
print(f"Start processing {len(files)}  power files \n\n")

all_interval_dfs = []
for idx, file in enumerate(files):
    
    print(f"Bezig met     file {idx}    van    {len(files)}   files...")
    
    base_file = os.path.splitext(file)[0]
    ftp = ftp_dict.get(base_file)
    if ftp is None:
        continue
    
    full_path = os.path.join(parquet_folder, file)
    
    df_file = pd.read_parquet(full_path)
    
    df_intervals = detect_intervals(df_file)
    
    df_intervals['interval_label'] = df_intervals.apply(intervals_mapper, axis=1, args=(ftp,))
    
    all_interval_dfs.append(df_intervals)

if all_interval_dfs:
    df_sql_intervals = pd.concat(all_interval_dfs, ignore_index=True)
else:
    df_sql_intervals = pd.DataFrame()
    
# ------------------------------------------------------------------------------------------
# ------------------------------ UPDATE OVERVIEW.sqlite ------------------------------
# ------------------------------------------------------------------------------------------

print("\nAlle bestanden verwerkt. Bezig met updaten van de database....")

conn = sqlite3.connect('overview.sqlite')

df_sql_intervals.to_sql('intervals', conn, if_exists='replace', index=False)

conn.close()

print("Database succesvol bijgewerkt met de mapped intervals")
