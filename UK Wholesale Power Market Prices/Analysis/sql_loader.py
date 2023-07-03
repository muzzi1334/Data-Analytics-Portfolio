import datetime
import pandas as pd
import psycopg2

# Establish connection to PostgreSQL database
conn = psycopg2.connect(
host="localhost",
database="uk-wholesale-power-prices",
user="postgres",
password="admin"
)

cur = conn.cursor()

# Create table/columns in PostgreSQL database if they don't exist
create_string = f"""CREATE TABLE IF NOT EXISTS imbal_mid (settlement_date DATE NOT NULL, settlement_period SMALLINT NOT NULL, ssp NUMERIC(10, 2), sbp NUMERIC(10, 2), bsad_default BOOLEAN, pdc char(1), rsp NUMERIC(10, 2), niv NUMERIC(10, 3), spa NUMERIC(10, 2), bpa NUMERIC(10, 2), rp NUMERIC(10, 2), rp_vol NUMERIC(10, 3), tsa_offer_vol NUMERIC(10, 3), tsa_bid_vol NUMERIC(10, 3), tst_offer_vol NUMERIC(10, 3), tst_bid_vol NUMERIC(10, 3), stp_offer_vol NUMERIC(10, 3), stp_bid_vol NUMERIC(10, 3), tsa_sell_vol NUMERIC(10, 3), tsa_buy_vol NUMERIC(10, 3), mid_price NUMERIC(10, 2), mid_vol NUMERIC(10, 3), UNIQUE (settlement_date, settlement_period));"""
cur.execute(create_string)

# Read the imbalance pricing CSV file into a Pandas DataFrame, remove static fields
df1 = pd.read_csv("imbalance pricing data.csv", header=None)
df1_edited = df1.drop(columns=0).groupby([1, 2], as_index=False).first().sort_values([1, 2])

# Read the market index CSV file into a Pandas DataFrame, remove static fields
df2 = pd.read_csv("market index data.csv", header=None)
df2_edited = df2.drop(columns=[0, 1]).groupby([2, 3], as_index=False).first().sort_values([2, 3])

# Join the DataFrames on the settlement_date + settlement_period fields
df_merged = pd.merge(df1_edited, df2_edited, how='inner', left_on=[1, 2], right_on=[2, 3]).drop(columns=['2_x', '2_y', '3_y']).fillna(0)

# Insert into PostgreSQL table
insert_string=f"""INSERT INTO imbal_mid VALUES (to_date(%s::TEXT, 'YYYYMMDD'), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
for _, row in df_merged.iterrows():
    cur.execute(insert_string, (row[1], row[2], row["3_x"], row["4_x"], row["5_x"], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20], row["4_y"], row["5_y"]))
print("Done pushing to DB")
    
# Commit and close database connection
conn.commit()
cur.close()
conn.close()