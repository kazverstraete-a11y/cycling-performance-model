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
        SELECT datum, eTRIMP, avg_temperature, startuur, total_ascent
        FROM Ritten
        WHERE eTRIMP > 0
        ORDER BY datum ASC
    '''
    df_trimp = pd.read_sql(query_trimp, conn)
    conn.close()
    return df_trimp
    
#------------------------------------------------------------------------------------------
def cross_training_fetcher(path):
    conn = sqlite3.connect(path)
    query = '''
        SELECT datum, SUM(eTRIMP) AS cross_eTRIMP
        FROM Ritten
        WHERE activity_cat = 'other'
        GROUP BY datum;
    '''
    df_CROSS = pd.read_sql(query, conn)
    conn.close()
    return df_CROSS
    
# ------------------------------------------------------------------------------------------
# ------------------------------------- MAIN LOOP -----------------------------------------
# ------------------------------------------------------------------------------------------
df_load = trimp_fetcher(db_path)

df_load['datum'] = pd.to_datetime(df_load['datum']).dt.normalize()
df_load['startuur'] = pd.to_datetime(df_load['startuur'], format='%H:%M:%S').dt.hour

agg_rules = {
    'eTRIMP': 'sum',
    'avg_temperature': 'mean',
    'startuur': 'min'
}

df_grouped = df_load.groupby('datum').agg(agg_rules)
df = df_grouped.asfreq('D').reset_index()

df['eTRIMP'] = df['eTRIMP'].fillna(0)
#forward fill voor temp en startuur op rustdagen
df['avg_temperature'] = df['avg_temperature'].ffill() 
df['startuur'] = df['startuur'].ffill()

#------------------------------------------------------------------------------------------
df_cross = cross_training_fetcher(db_path)

df_cross['datum'] = pd.to_datetime(df_cross['datum']).dt.normalize()

df = pd.merge(df, df_cross, on='datum', how='left')
df['cross_eTRIMP'] = df['cross_eTRIMP'].fillna(0)

df['cross_load_yesterday'] = df['cross_eTRIMP'].shift(1).fillna(0)

df.drop(columns=['cross_eTRIMP'], inplace=True)

# --------------------------------         E-TRIMP METRICS   -------------------------------
# Banister model logic
df['CTL-1'] = df['eTRIMP'].ewm(halflife=42, adjust=False).mean()
df['ATL-1'] = df['eTRIMP'].ewm(halflife=7, adjust=False).mean()
df['FORM-1'] = df['CTL-1'] - df['ATL-1']

df['CTL'] = df['CTL-1'].shift(1).fillna(0)
df['ATL'] = df['ATL-1'].shift(1).fillna(0)
df['FORM'] = df['FORM-1'].shift(1).fillna(0)

# ------------------------------------------------------------------------------------------
# ------------------------------------ UPDATE OVERVIEW.SQLITE -----------------------------
# ------------------------------------------------------------------------------------------

conn = sqlite3.connect(db_path)

df.to_sql('calendar', conn, if_exists='replace', index=False)

conn.close()

print("\n\nNew table 'calendar' added to database")
