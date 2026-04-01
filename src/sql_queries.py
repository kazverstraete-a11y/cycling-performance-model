import sqlite3

# ---------------------------------------------------------
# -------------------------- CONNECT ----------------------
# ---------------------------------------------------------
db = 'bert_2_overview.sqlite'
conn = sqlite3.connect(db)
cur = conn.cursor()

# ---------------------------------------------------------
# -------------------------- Basic QUERIES ----------------
# ---------------------------------------------------------

Q_col_names = [
    "ALTER TABLE Ritten RENAME COLUMN unknown_110 TO activity_type",
    "ALTER TABLE Ritten RENAME COLUMN unknown_0 TO 'VO2max'",
    "ALTER TABLE Ritten RENAME COLUMN unknown_1 TO 'age'",
    "ALTER TABLE Ritten RENAME COLUMN 'unknown_11' TO 'thhr'",
    "ALTER TABLE Ritten RENAME COLUMN total_distance TO 'distance_km'",
    "ALTER TABLE Ritten ADD COLUMN 'activity_cat' TEXT",
    "ALTER TABLE Ritten ADD COLUMN 'datum' TEXT",
    "ALTER TABLE Ritten ADD COLUMN 'startuur' TEXT"
]
for q in Q_col_names:
    try:
        cur.execute(q)
    except sqlite3.OperationalError:
        print(f"Overslaan (reeds aangepast): {q[:40]}...")
# ---------------------------------------------------------
Q_dt = '''
    UPDATE Ritten
    SET 
        datum = date(start_time),
        startuur = time(start_time);
'''
cur.execute(Q_dt)
# ---------------------------------------------------------
Q_km = '''
    UPDATE Ritten
    SET distance_km = ROUND(distance_km/1000, 2)
    WHERE distance_km > 100;
'''
cur.execute(Q_km)
# ---------------------------------------------------------
Q_bike_indoor = '''
    UPDATE Ritten
    SET activity_type = 'Bike Indoor'
    WHERE manufacturer IN ('zwift', 'trainer_road');
'''
cur.execute(Q_bike_indoor)
# ---------------------------------------------------------
Q_unknown = '''
    UPDATE Ritten
    SET activity_type = 'unknown'
    WHERE activity_type IS NULL;
'''
cur.execute(Q_unknown)
# ---------------------------------------------------------
Q_activity_cat = '''
    UPDATE Ritten
    SET activity_cat = CASE
	WHEN activity_type IN ('Road Bike', 'Bike', 'COMMUTE', 'GRAVEL',
                        'Bike Commute', 'Gravel Bike', 'MTB', 'ROAD', 'Bike Indoor') THEN 'cycling'
	ELSE 'other'
    END;
'''
cur.execute(Q_activity_cat)
# ---------------------------------------------------------
Q_temp_outdoor = '''
    UPDATE Ritten
    SET avg_temperature  = CASE strftime('%m', start_time)
        WHEN '01' THEN -3
        WHEN '02' THEN -3
        WHEN '03' THEN 1
        WHEN '04' THEN 5
        WHEN '05' THEN 11
        WHEN '06' THEN 15
        WHEN '07' THEN 18
        WHEN '08' THEN 16
        WHEN '09' THEN 11
        WHEN '10' THEN 7
        WHEN '11' THEN 1
        WHEN '12' THEN -3
    END
    WHERE avg_temperature IS NULL
    AND activity_type IN ('Run', 'Hike', 'Road Bike', 'Bike Commute', 'Ski', 'XC Classic Ski',
                        'Walk', 'Bike', 'Backcountry Ski', 'Gravel Bike', 'MTB');
'''
cur.execute(Q_temp_outdoor)
# ---------------------------------------------------------
Q_temp_indoor = '''
    UPDATE Ritten
    SET avg_temperature = CASE
        WHEN activity_type IN ('Cardio', 'HIIT', 'Strength', 'Yoga', 'Bike Indoor', 'Climb Indoor') THEN 20
        WHEN activity_type = 'unknown' THEN 10
        ELSE avg_temperature
    END;
'''
cur.execute(Q_temp_indoor)
# ---------------------------------------------------------
# --------------------- FORWARD & BW FILLS ----------------
# ---------------------------------------------------------
Q_ff_threshold_power = '''
    UPDATE Ritten AS main
    SET threshold_power = (
        SELECT r2.threshold_power
        FROM Ritten AS r2
        WHERE r2.activity_cat = 'cycling'
          AND r2.threshold_power IS NOT NULL
          AND (r2.datum < main.datum OR (r2.datum = main.datum AND r2.startuur < main.startuur))
        ORDER BY r2.datum DESC, r2.startuur DESC
        LIMIT 1
    )
    WHERE main.activity_cat = 'cycling' 
        AND main.threshold_power IS NULL;
'''
cur.execute(Q_ff_threshold_power)
# ---------------------------------------------------------
Q_ff_threshold_hr = '''
    UPDATE Ritten AS main
    SET thhr = (
        SELECT r2.thhr
        FROM Ritten AS r2
        WHERE r2.activity_cat = 'cycling'
            AND r2.thhr IS NOT NULL
            AND (r2.datum < main.datum OR (r2.datum = main.datum AND r2.startuur < main.startuur))
        ORDER BY r2.datum DESC, r2.startuur DESC
        LIMIT 1
    )
    WHERE main.activity_cat = 'cycling'
        AND main.thhr IS NULL;
'''
cur.execute(Q_ff_threshold_hr)
# ---------------------------------------------------------
Q_bw_threshold_hr = '''
    UPDATE Ritten AS main
    SET thhr = (
        SELECT r2.thhr
        FROM Ritten AS r2
        WHERE r2.activity_cat = 'cycling'
            AND r2.thhr IS NOT NULL
            AND (r2.datum > main.datum OR (r2.datum = main.datum AND r2.startuur > main.startuur))
        ORDER BY r2.datum DESC, r2.startuur DESC
        LIMIT 1
    )
    WHERE main.activity_cat = 'cycling'
        AND main.thhr IS NULL;
'''
cur.execute(Q_bw_threshold_hr)
# ---------------------------------------------------------
Q_ff_VO2max = '''
    UPDATE Ritten AS main
    SET VO2max = (
        SELECT r2.VO2max
        FROM Ritten AS r2
        WHERE r2.activity_cat = 'cycling'
            AND r2.VO2max IS NOT NULL
            AND (r2.datum < main.datum OR (r2.datum = main.datum AND r2.startuur < main.startuur))
        ORDER BY r2.datum DESC, r2.startuur DESC
        LIMIT 1
    )
    WHERE main.activity_cat = 'cycling'
        AND main.VO2max IS NULL;
'''
cur.execute(Q_ff_VO2max)
# ---------------------------------------------------------
Q_bf_VO2max = '''
    UPDATE Ritten AS main
    SET VO2max = (
        SELECT r2.VO2max
        FROM Ritten AS r2
        WHERE r2.activity_cat = 'cycling'
            AND r2.VO2max IS NOT NULL
            AND (r2.datum > main.datum OR (r2.datum = main.datum AND r2.startuur > main.startuur))
        ORDER BY r2.datum DESC, r2.startuur DESC
        LIMIT 1
    )
    WHERE main.activity_cat = 'cycling'
        AND main.VO2max IS NULL;
'''
cur.execute(Q_bf_VO2max)
# ---------------------------------------------------------
# ---------------------------------------------------------
# ---------------------------------------------------------

conn.commit()
conn.close()

print(f"{db}   updated, all queries executed!")

