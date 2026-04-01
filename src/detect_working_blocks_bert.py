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
#-------------------------------------- FUNCTIONS -----------------------------------------
#------------------------------------------------------------------------------------------

def metadata_fetcher(db_path):
    conn = sqlite3.connect(db_path)
    
    query = """ 
            SELECT threshold_power, thhr, source_file, avg_power, activity_cat
            FROM Ritten 
    """
    
    df = pd.read_sql(query, conn)
    
    metadata_dict = {}
    power_files = []
    no_power_files = []
    
    for _, row in df.iterrows():
        if not row['source_file']: continue
        
        name_only = os.path.splitext(row['source_file'])[0]
        
        metadata_dict[name_only] = {
            'ftp': row['threshold_power'],
            'thhr': row['thhr']
        }
        
        parquet_filename = name_only+'.parquet'
        if row['avg_power'] and row['avg_power'] > 0 and row['activity_cat'] == 'cycling':
            power_files.append(parquet_filename)
        else: 
            no_power_files.append(parquet_filename)
    
    conn.close()

    return {'power': power_files, 'no_power': no_power_files}, metadata_dict

#----------------------------------- POWER CALCS TO PARQUET ----------------------------------
def detect_work_blocks(df, ATHLETE_FTP):
    
    power_smoothed = df['power'].rolling(window=5, center=True, min_periods=1).mean()
    df['is_werkblok'] = (power_smoothed >= (AER_DREM * ATHLETE_FTP)).astype(int)
    
    return df

#------------------------------------------  HR CALCS ----------------------------------------
def calc_hr_stress(df, ATHLETE_THHR):
    hr_labels = ['z1', 'z2', 'z3', 'z4','z5']
    hr_bins = [
        0,
        ATHLETE_THHR*0.68,
        ATHLETE_THHR*0.83, 
        ATHLETE_THHR*0.94, 
        ATHLETE_THHR*1.05,
        250
    ]
    
    default_z1_hr = ATHLETE_THHR * 0.54999
    if 'heart_rate' not in df.columns:
        df['heart_rate'] = default_z1_hr
    
    df['heart_rate'] = pd.to_numeric(df['heart_rate'], errors='coerce')
    
    #fillnans
    df['heart_rate'] = df['heart_rate'].fillna(135)
        
    df['hr_zone'] = pd.cut(df['heart_rate'], bins=hr_bins, labels=hr_labels)
    df['eTRIMP_points'] = df['hr_zone'].map(TRIMP_WEIGHTS)

    df['eTRIMP_points'] = pd.to_numeric(df['eTRIMP_points'], errors='coerce').fillna(0)

    return df

# ------------------------------------  Folders  -------------------------------------------
db_path = 'bert_2_overview.sqlite'
parquet_folder = 'parquet_files'
# ------------------------------------   Lists  --------------------------------------------
power_files_with_errors = []
hr_files_with_errors = []
sql_db_updates=[]

# ----------------------------------- Magic Numbers ----------------------------------------
TRIMP_WEIGHTS = {'z1': 1, 'z2': 2, 'z3': 3, 'z4': 4, 'z5':5}
AER_DREM = 0.7999999999999

# -----------------------------------    Initiate  -----------------------------------------
files, metadata_dict = metadata_fetcher(db_path)

# ------------------------------------------------------------------------------------------
# ------------------------------------- MAIN LOOP ------------------------------------------
# ------------------------------------------------------------------------------------------

# ------------------------------------  POWER RIDES ----------------------------------------
print(f"Start processing {len(files['power'])}  power files - {len(files['no_power'])}  heart rate files\n\n")
print(f"Power files....")

for idx, file in enumerate(files['power']):
    
    base_file = os.path.splitext(file)[0]
    
    if base_file in metadata_dict: 
        ftp = metadata_dict[base_file].get('ftp') or 250
        thhr = metadata_dict[base_file].get('thhr')
        if not thhr or thhr == 0:
            thhr=170
            print(f"---------- ERROR: Used default thhr of 170bpm for {file}")
    
    full_path = os.path.join(parquet_folder, file)
    try:
        df = pd.read_parquet(full_path)

        if 'power' not in df.columns:
            raise KeyError(f"Kolom 'power' ontbreekt in {file}")

        df = detect_work_blocks(df, ftp)
        df = calc_hr_stress(df, thhr)

        df.to_parquet(full_path, index=False)

        source_file = base_file + '.fit'
        etrimp_score = ((df['eTRIMP_points'].sum()) / 60).round(2)

        sql_db_updates.append((etrimp_score, source_file))

        print(f"*Bestand {idx}/{len(files['power'])}:    {file}     succesvol bijgewerkt")
    
    except Exception as e:
        error_msg = f"ERROR in {file}: {str(e)}"
        power_files_with_errors.append(error_msg)
        print(f"!!! {error_msg}")
    
# --- EINDRAPPORTAGE ---
print(f"\n\n{'='*30}")
print(f"VERWERKING VOLTOOID")
print(f"Succesvol: {len(sql_db_updates)}")
print(f"Fouten: {len(power_files_with_errors)}")
print(f"{'='*30}")

if power_files_with_errors:
    print("\nLijst met problematische bestanden:")
    for error in power_files_with_errors:
        print(f" - {error}")

# ------------------------------------ NO POWER ACTIVITIES ----------------------------------------
print(f"\n\nHeart rate files....")

for idx, file in enumerate(files['no_power']):
    base_file = os.path.splitext(file)[0]
    
    if base_file in metadata_dict:
        thhr = metadata_dict[base_file]['thhr']
        if not thhr or thhr == 0:
            thhr=170
            hr_files_with_errors.append(file)
            print(f"---------- ERROR: Used default thhr of 170bpm for {file}")
    
    full_path = os.path.join(parquet_folder, file)
    
    try:
        df_no_power = pd.read_parquet(full_path)
        df = calc_hr_stress(df_no_power, thhr)

        df.to_parquet(full_path, index=False)

        source_file = base_file + '.fit'
        etrimp_score = ((df['eTRIMP_points'].sum()) / 60).round(2)

        sql_db_updates.append((etrimp_score, source_file))

        print(f"*Bestand {idx}/{len(files['no_power'])}:    {file}     succesvol bijgewerkt")
    
    except Exception as e:
        error_msg = f"ERROR in {file}: {str(e)}"
        hr_files_with_errors.append(error_msg)
        print(f"!!! {error_msg}")

print(f"\n\n{'='*30}")
print(f"VERWERKING VOLTOOID")
if hr_files_with_errors:
    print("\nLijst met problematische bestanden:")
    for error in hr_files_with_errors:
        print(f" - {error}")
    
# ------------------------------------------------------------------------------------------
# ------------------------------ UPDATE ALEX_OVERVIEW.sqlite -------------------------------
# ------------------------------------------------------------------------------------------

print("\n\n\nAlle bestanden verwerkt. Bezig met updaten van de database....")

conn = sqlite3.connect('bert_2_overview.sqlite')
cur = conn.cursor()

try:
    cur.execute("ALTER TABLE Ritten ADD COLUMN eTRIMP REAL")
    print("\nNieuwe kolom 'eTRIMP' succesvol toegevoegd.")
except sqlite3.OperationalError:
    print("\nKolom 'eTRIMP' bestaat al, klaarmaken voor update...")

update_query = """
    UPDATE Ritten
    SET eTRIMP = ?
    WHERE source_file = ?
"""
cur.executemany(update_query, sql_db_updates)

print("Database succesvol bijgewerkt met eTRIMP Scores")
# ------------------------------------------------------------------------------------------
# ------------------------------  CLEANUP (FALLBACK VOOR GLITCHES) -------------------------
# ------------------------------------------------------------------------------------------
print("\nBezig met het opschonen van lege bestanden (watch glitches)...")

cleanup_query = """
    UPDATE Ritten
    SET eTRIMP = 10
    WHERE eTRIMP IS NULL
"""
cur.execute(cleanup_query)
# ------------------------------------------------------------------------------------------
conn.commit()
conn.close()

print("Database succesvol bijgewerkt met eTRIMP Scores én fallback voor lege bestanden!")