import pandas as pd
import os
import sqlite3

#------------------------------------------------------------------------------------------
# --------------------------------------- INITIATE ----------------------------------------
#------------------------------------------------------------------------------------------

db_path = 'overview.sqlite'

#------------------------------------------------------------------------------------------
#-------------------------------------- FUNCTIONS(+ Mappings) -----------------------------
#------------------------------------------------------------------------------------------
def trimp_fetcher(path):
    conn = sqlite3.connect(path)
    query_trimp = '''
        SELECT datum, eTRIMP, training_stress_score, avg_temperature, startuur
        FROM Ritten
        WHERE eTRIMP > 0
        ORDER BY datum ASC
    '''
    df_trimp = pd.read_sql(query_trimp, conn)
    conn.close()
    return df_trimp

def strength_date_fetcher(path):
    conn = sqlite3.connect(path)
    query = '''
        SELECT datum, activity_type
        FROM Ritten
        WHERE activity_type = 'Krachtsport'
    '''
    df_kracht = pd.read_sql(query, conn)
    
    return df_kracht['datum'].tolist()
    
#------------------------------------------------------------------------------------------
# ------------------------------------- MAIN LOOP -----------------------------------------
#------------------------------------------------------------------------------------------
df_load = trimp_fetcher(db_path)
df_load['datum'] = pd.to_datetime(df_load['datum']).dt.normalize()

df_load['start_uur'] = pd.to_datetime(df_load['startuur']).dt.hour

agg_rules = {
    'eTRIMP': 'sum',
    'training_stress_score': 'sum',
    'avg_temperature': 'mean',
    'start_uur': 'min'
}

df_grouped = df_load.groupby('datum').agg(agg_rules)
df = df_grouped.asfreq('D').reset_index()

df['eTRIMP'] = df['eTRIMP'].fillna(0)
df['training_stress_score'] = df['training_stress_score'].fillna(0)
#forward fill voor temp en startuur op rustdagen
df['avg_temperature'] = df['avg_temperature'].ffill() 
df['start_uur'] = df['start_uur'].ffill()

#------------------------------------------------------------------------------------------
kracht_datums = strength_date_fetcher(db_path)
df['strength'] = 0
df['strength_yesterday'] = 0

df = df.set_index('datum')

for date in kracht_datums:
    dt_date = pd.to_datetime(date)
    if date in df.index:
        df.at[date, 'strength'] = 1

df['strength_yesterday'] = df['strength'].shift(1).fillna(0).astype(int)
df.reset_index(inplace=True)
df.drop(columns=['strength'], inplace=True)

#--------------------------------         E-TRIMP METRICS   -------------------------------
# Banister model logic
df['CTL-1'] = df['eTRIMP'].ewm(halflife=42, adjust=False).mean()
df['ATL-1'] = df['eTRIMP'].ewm(halflife=7, adjust=False).mean()
df['FORM-1'] = df['CTL-1'] - df['ATL-1']

df['CTL'] = df['CTL-1'].shift(1).fillna(0)
df['ATL'] = df['ATL-1'].shift(1).fillna(0)
df['FORM'] = df['FORM-1'].shift(1).fillna(0)

#--------------------------------     TRADITIONAL CSS METRICS      ------------------------

df['CTL_TSS-1'] = df['training_stress_score'].ewm(halflife=42, adjust=False).mean()
df['ATL_TSS-1'] = df['training_stress_score'].ewm(halflife=7, adjust=False).mean()
df['FORM-1_TSS'] = df['CTL_TSS-1'] - df['ATL_TSS-1']
df['FORM_TSS'] = df['FORM-1_TSS'].shift(1).fillna(0)

#------------------------------------------------------------------------------------------
# ------------------------------------ UPDATE OVERVIEW.SQLITE -----------------------------
#------------------------------------------------------------------------------------------

conn = sqlite3.connect(db_path)

df.to_sql('calendar', conn, if_exists='replace', index=False)

conn.close()

print("\n\nNew table 'calendar' added to database")




