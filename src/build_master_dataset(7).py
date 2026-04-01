import pandas as pd
import os
import sqlite3

# --- INITIATE ---
db_path = 'overview.sqlite'

# -----------------------------------------------------------------------------------------
# ------------------------------------- FETCH DATA  ---------------------------------------
# -----------------------------------------------------------------------------------------
print(f"\n Retrieving data from SQL: {db_path}")
conn = sqlite3.connect(db_path)

# Calendar
calendar_df = pd.read_sql('SELECT datum, cross_load_yesterday, avg_temperature, startuur, CTL, ATL, FORM FROM calendar', conn)
calendar_df['datum'] = pd.to_datetime(calendar_df['datum']).dt.date

calendar_df['cross_7d_ewma'] = calendar_df['cross_load_yesterday'].ewm(halflife=7, adjust=False).mean().shift(1).fillna(0)
calendar_df['cross_28d_ewma'] = calendar_df['cross_load_yesterday'].ewm(halflife=28, adjust=False).mean().shift(1).fillna(0)
calendar_df['cross_42_ewma'] = calendar_df['cross_load_yesterday'].ewm(halflife=42, adjust=False).mean().shift(1).fillna(0)

# Intervals
df_load = pd.read_sql('SELECT * FROM intervals', conn)
conn.close()

df_load['starttijd'] = pd.to_datetime(df_load['starttijd'], utc=True).dt.normalize()
df_load['datum'] = df_load['starttijd'].dt.date

# -----------------------------------------------------------------------------------------
# -----------------------------   EXTRACT TARGET VARIABLES   ------------------------------
# -----------------------------------------------------------------------------------------
print("\n Extracting ML targets: Decoupling (Base) and EF (Race form)...")

z2_blocks = df_load[df_load['interval_label'] == 'Z2_Decoupling_Block']
daily_decoupling = z2_blocks.groupby('datum')['decoupling'].mean().reset_index()
daily_decoupling.rename(columns={'decoupling': 'target_decoupling'}, inplace=True)

clean_race_labels = ['Threshold', 'Tempo', 'Sweet_Spot']
race_blocks = df_load[df_load['interval_label'].isin(clean_race_labels)]
daily_race_ef = race_blocks.groupby('datum')['EF'].mean().reset_index()
daily_race_ef.rename(columns={'EF': 'target_race_EF'}, inplace=True)

# -----------------------------------------------------------------------------------------
# --------------------------------    FEATURE ENG: EWMAs    -------------------------------
# -----------------------------------------------------------------------------------------
print("\nCalculating EWMAs on intervals...")

# Aggregate per day
df = df_load.groupby(['datum', 'interval_label'])['duur'].sum().unstack(fill_value=0)
df = df.asfreq('D').fillna(0).reset_index()
df.columns.name = None

# Calculate EWMA's
ewma_cols = [col for col in df.columns if col != 'datum']
for col in ewma_cols:
    df[f"{col}_7d_ewma"] = df[col].ewm(halflife=7, adjust=False).mean().shift(1).fillna(0)
    df[f"{col}_28d_ewma"] = df[col].ewm(halflife=28, adjust=False).mean().shift(1).fillna(0)
    df[f"{col}_42d_ewma"] = df[col].ewm(halflife=42, adjust=False).mean().shift(1).fillna(0)

# -----------------------------------------------------------------------------------------
# ----------------------------------       MERGE & CLEANUP       --------------------------
# -----------------------------------------------------------------------------------------
print("\n Merging calendar, training and form (EF) dataframes... ")

df['datum'] = pd.to_datetime(df['datum']).dt.date

ml_df = pd.merge(calendar_df, df, on='datum', how='left')

# Add decoupling
if not daily_decoupling.empty:
    ml_df = pd.merge(ml_df, daily_decoupling, on='datum', how='left')
else:
    ml_df['target_decoupling'] = None
    
# Voeg Target 2 (Race EF) toe
if not daily_race_ef.empty:
    ml_df = pd.merge(ml_df, daily_race_ef, on='datum', how='left')
else:
    ml_df['target_race_EF'] = None

# Vul alleen de NaN's van trainingen met 0 (EF blijft NaN als er niet getraind is!)
all_interval_features = [col for col in df.columns if col != 'datum']
ml_df.fillna({col: 0 for col in all_interval_features}, inplace=True)

# -----------------------------------------------------------------------------------------
# -----------------------------       SQL & PARQUET EXPORT       --------------------------
# -----------------------------------------------------------------------------------------

conn = sqlite3.connect(db_path)
ml_df.to_sql('final_table', conn, if_exists='replace', index=False)
conn.close()

ml_df['datum'] = pd.to_datetime(ml_df['datum'])
ml_df.to_parquet('bert_ml_dataset_final.parquet', index=False)

print("\n FINISHED:\n -------> SQL updated\n -------> Dataset stored as: 'ml_dataset_final.parquet'.")
