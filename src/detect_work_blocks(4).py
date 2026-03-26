import pandas as pd
import os
import sqlite3

#------------------------------------------------------------------------------------------
#-------------------------------------- MAGIC NUMBERS -------------------------------------
#------------------------------------------------------------------------------------------

# NOTE:
# HR thresholds (TH_HR, MAX_HR) are currently hardcoded.
# In a more general implementation, these should be dynamically derived per athlete.

AER_DREM = 0.78
TH_HR = 171
MAX_HR = 188

#TRIMP WEIGHTING
TRIMP_WEIGHTS = {'z1': 1, 'z2': 2, 'z3': 3, 'z4': 4, 'z5':5}
hr_zones_limits = {
    'z1': 0.59, 'z2': 0.69, 'z3': 0.79, 'z4': 0.89, 'z5': 1.00
}

hr_labels = list(hr_zones_limits.keys())
hr_bins = [0] + [pct * MAX_HR for pct in hr_zones_limits.values()]

#------------------------------------------------------------------------------------------
#-------------------------------------- FUNCTIONS -----------------------------------------
#------------------------------------------------------------------------------------------

def file_ftp_fetcher(db_path):
    #power files
    conn = sqlite3.connect(db_path)
    query_power = (""" SELECT threshold_power, source_file FROM Ritten 
                WHERE activity_type = 'WIELRENNEN' AND avg_power > 0
                """)
    df_power = pd.read_sql(query_power, conn)
    
    ftp_dict = {}
    for _, row in df_power.iterrows():
        name_only = os.path.splitext(row['source_file'])[0]
        ftp_dict[name_only] = row['threshold_power']
    
    power_files = [os.path.splitext(f)[0] + '.parquet' for f in df_power['source_file']]
    
    # no power files
    cur = conn.cursor()
    cur.execute(""" SELECT source_file FROM ritten WHERE activity_type = 'WIELRENNEN'
                    AND (avg_power IS NULL OR avg_power = 0)
                """)
    no_power_fit = [row[0] for row in cur.fetchall()]
    no_power_files = [os.path.splitext(f)[0]+'.parquet' for f in no_power_fit]
    
    files = {'power': power_files, 'no_power': no_power_files}
    conn.close()
    
    return files, ftp_dict


def detect_work_blocks(df, current_ftp):
    
    #---------- POWER CALCS ------------
    power_smoothed = df['power'].rolling(window=5, center=True, min_periods=1).mean()
    df['is_werkblok'] = (power_smoothed >= (AER_DREM * current_ftp)).astype(int)
    
    #---------- HR CALCS --------------
    df['heart_rate'] = pd.to_numeric(df['heart_rate'], errors='coerce')
    df['hr_zone'] = pd.cut(df['heart_rate'], bins=hr_bins, labels=hr_labels)
    df['eTRIMP_points'] = df['hr_zone'].map(TRIMP_WEIGHTS)
    df['eTRIMP_points'] = pd.to_numeric(df['eTRIMP_points'], errors='coerce').fillna(0)
    
    return df

def calc_hr_stress(df):
    #----------- HR PARAMS -----------
    if 'heart_rate' in df.columns:
        df['heart_rate'] = pd.to_numeric(df['heart_rate'], errors='coerce')
        df['hr_zone'] = pd.cut(df['heart_rate'], bins=hr_bins, labels=hr_labels)
        df['eTRIMP_points'] = df['hr_zone'].map(TRIMP_WEIGHTS)
        df['eTRIMP_points'] = pd.to_numeric(df['eTRIMP_points'], errors='coerce').fillna(0)
        
    else: 
        df['eTRIMP_points'] = 0
    
    return df

#------------------------------------------------------------------------------------------
# ------------------------------------- MAIN LOOP -----------------------------------------
#------------------------------------------------------------------------------------------

#--- INITIATE ---
db_path = 'overview.sqlite'
parquet_folder = 'parquet_files'

files, ftp_dict = file_ftp_fetcher(db_path)

sql_db_updates=[]

#--- UPDATE POWER FILES --- 
print(f"Start processing {len(files['power'])}  power files - {len(files['no_power'])}  heart rate files\n\n")
print(f"Power files....")

for idx, file in enumerate(files['power']):
    
    base_file = os.path.splitext(file)[0]
    ftp = ftp_dict.get(base_file)
    
    full_path = os.path.join(parquet_folder, file)
    
    df_power = pd.read_parquet(full_path)
    df = detect_work_blocks(df_power, ftp)
    df.to_parquet(full_path, index=False)
    
    print(f"*Bestand {idx}/{len(files['power'])}:    {file}     succesvol bijgewerkt")
    
    source_file = base_file + '.fit'
    etrimp_score = ((df['eTRIMP_points'].sum()) / 60).round(2)
    
    sql_db_updates.append((etrimp_score, source_file))
                   
print(f"\n\nAll power files processed!")

#--- UPDATE HR FILES ----
print(f"\n\nHeart rate files....")
for idx, file in enumerate(files['no_power']):
    base_file = os.path.splitext(file)[0]
    
    full_path = os.path.join(parquet_folder, file)
    
    df_no_power = pd.read_parquet(full_path)
    df = calc_hr_stress(df_no_power)
    df.to_parquet(full_path, index=False)
    
    print(f"*Bestand {idx}/{len(files['no_power'])}:    {file}     succesvol bijgewerkt")
    
    source_file = base_file + '.fit'
    etrimp_score = ((df['eTRIMP_points'].sum()) / 60).round(2)
    
    sql_db_updates.append((etrimp_score, source_file))
    
print(f"\n\nAll heart rate files processed!")

#------------------------------------------------------------------------------------------
# ----------------------------------   UPDATE SQL DB   ------------------------------------
#------------------------------------------------------------------------------------------

print("\n\n\nAlle bestanden verwerkt. Bezig met updaten van de database....")

conn = sqlite3.connect('overview.sqlite')
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

conn.commit()
conn.close()

print("Database succesvol bijgewerkt met eTRIMP Scores")
