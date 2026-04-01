import fitdecode
import pandas as pd
import os
from tqdm import tqdm

# -------------------------    Folders  ------------------------
DATA_DIR = 'files'
OUTPUT_DIR = 'parquet_files'

if not os.path.exists(OUTPUT_DIR): 
    os.makedirs(OUTPUT_DIR)


# ----------------------------------------------------------------
# ------------------------------------- MAIN LOOP ----------------
# ----------------------------------------------------------------
count=0
print(f"Start met het verwerken van {DATA_DIR}....")

files = [f for f in os.listdir(DATA_DIR) if f.lower().endswith('.fit')]
print(f"Start met het verwerken van {len(files)}...")

for filename in tqdm(files, desc="Verwerking"):
    path = os.path.join(DATA_DIR, filename)
    
    data = []
    if os.path.isfile(path):
        try:
            with fitdecode.FitReader(path) as fit:
                for frame in fit:
                    if frame.frame_type == fitdecode.FIT_FRAME_DATA and frame.name == 'record':
                        row = {field.name: field.value for field in frame.fields}

                        row['source_file'] = filename
                        data.append(row)

            if data:
                df = pd.DataFrame(data)
                for col in df.columns:
                    if df[col].dtype == 'object':
                        df[col] = df[col].astype(str)

                output_name = filename.rsplit('.', 1)[0] + '.parquet'
                df.to_parquet(os.path.join(OUTPUT_DIR, output_name))
                count += 1

        except Exception as e:
            print(f"\nError bij {filename}: {e}")
        
# ------------------------------------------------------------------------------------------
print("\n\n")
print("-"*50)
print(f"Klaar! Alle {count} bestanden staan in de {OUTPUT_DIR} map.")




