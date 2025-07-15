import requests
import zipfile
import io
import pandas as pd
import numpy as np



# Step 1: Define the static list of archive URLs with year labels
urls = {
    2022: "https://www.uktradeinfo.com/media/gejdh3ug/pref_2022archive.zip",
    2023: "https://www.uktradeinfo.com/media/y53jjpug/pref_2023archive.zip",
    2024: "https://www.uktradeinfo.com/media/vwnbmjbb/pref_2024archive.zip",
    2025: "https://www.uktradeinfo.com/media/jv3fmnwd/pref_2025archive.zip"
}

dfs = []  # list of tuples: (year, csv_filename, DataFrame)

# Step 2: Loop through each URL, download and extract
for year, url in urls.items():
    print(f"🔽 Downloading archive for {year}...")
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"❌ Failed to fetch archive for {year}")
        continue

    zip_bytes = io.BytesIO(response.content)
    with zipfile.ZipFile(zip_bytes, "r") as z:
        for csv_file in z.namelist():
            if csv_file.endswith(".csv"):
                with z.open(csv_file) as f:
                    df = pd.read_csv(f, encoding='utf-8', low_memory=False)
                    dfs.append((year, csv_file, df))

print("\n✅ All data successfully scraped and unpacked.")


data = pd.concat([df for _, _, df in dfs], ignore_index=True)

data['comcode'] = data['comcode'].astype(str)
data['perref'] = data['perref'].astype(str)
data['statvalue'] = data['statvalue'].astype('int32')
data['month'] = data['perref'].str.slice(4,6)
data['year'] = data['perref'].str.slice(0,4)
data.head(10)

# Step 3: Clean and process the data
# comcode clean
# some comcodes have their leading '0' removed due to csv formatting. These need adding on to string length 7 (all digits should be 8 lenght)

data['len'] = data['comcode'].str.len()
data['commodity_code'] = data['comcode'].str.zfill(8)
# place next to comcode column to QA
col = data.pop('commodity_code')
data.insert(2, 'commodity_code', col)
data['len2'] = data['commodity_code'].str.len()
data.info()

# drop columns not needed

data = data.drop(['len', 'len2', 'comcode'], axis=1)

# fill NAs as 'NA for Namibia
data[['cooalpha', 'codalpha']] = data[['cooalpha', 'codalpha']].fillna("'NA")


data2 = data.copy()
data2['imports_total'] = data2['statvalue']
data2['imports_exc_special_regime'] = np.where(data2['statreg'] == 1, data2['statvalue'], 0)
data2['eligibility_mfn'] = np.where((data2['eligibility'] == 'e1') & (data2['statreg'] == 1), data2['statvalue'], 0)
data2['eligibility_gsp'] = np.where((data2['eligibility'] == 'e2') & (data2['statreg'] == 1) & (~data2['use'].isin(['u10', 'uZZ'])), data2['statvalue'], 0)
data2['eligibility_fta'] = np.where((data2['eligibility'] == 'e3') & (data2['statreg'] == 1) & (~data2['use'].isin(['u10', 'uZZ'])), data2['statvalue'], 0)
data2['eligibility_combined_pref'] = np.where((data2['eligibility'] == 'e5') & (data2['statreg'] == 1) & (~data2['use'].isin(['u10', 'uZZ'])), data2['statvalue'], 0)
data2['eligibility_unknown'] = np.where((data2['eligibility'] == 'eZ') & (data2['statreg'] == 1), data2['statvalue'], 0)
data2['eligibility__pref_unknown'] = np.where((data2['eligibility'].isin(['e2','e3','e5'])) & (data2['statreg'] == 1) & (data2['use'] == 'uzz'), data2['statvalue'], 0)
data2['use_mfn_0'] = np.where((data2['use'] == "u10") & (data2['statreg'] == 1), data2['statvalue'], 0)
data2['use_mfn_non_0'] = np.where((data2['use'] == "u11") & (data2['statreg'] == 1), data2['statvalue'], 0)
data2['use_gsp_0'] = np.where((data2['use'] == "u20") & (data2['statreg'] == 1), data2['statvalue'], 0)
data2['use_gsp_non_0'] = np.where((data2['use'] == "u21") & (data2['statreg'] == 1), data2['statvalue'], 0)
data2['use_fta_0'] = np.where((data2['use'] == "u30") & (data2['statreg'] == 1), data2['statvalue'], 0)
data2['use_fta_non_0'] = np.where((data2['use'] == "u31") & (data2['statreg'] == 1), data2['statvalue'], 0)
data2['use_unknown'] = np.where((data2['use'] == "uzz") & (data2['statreg'] == 1), data2['statvalue'], 0)
data2['eligibility_pref'] = np.where((data2['eligibility'].isin(['e2','e3','e5'])) & (data2['statreg'] == 1) & (data2['use'].isin(['u10','u11'])), data2['statvalue'], 0)
data2['use_pref'] = np.where((data2['use'].isin(['u20','u21','u30','u31'])) & (data2['statreg'] == 1), data2['statvalue'], 0)

data2

# Step 4: Match metadata from following sources:

# need to connect to DW datasets to match in for pipeline 

import pandas
import psycopg2
import sqlalchemy

engine = sqlalchemy.create_engine('postgresql://', execution_options={"stream_results": True})
chunks = pandas.read_sql(sqlalchemy.text("""SELECT * FROM \"hmrc\".\"country_list\" """), engine, chunksize=10000)
for chunk in chunks:
    display(chunk)

# second SQL dataset


engine = sqlalchemy.create_engine('postgresql://', execution_options={"stream_results": True})
chunks = pandas.read_sql(sqlalchemy.text("""SELECT * FROM \"hmrc\".\"comcode_descriptions\" """), engine, chunksize=10000)
for chunk in chunks:
    display(chunk)
 

# Step 5: Save the processed data to a CSV file

data_csv = data2.head(1000)
data_csv.to_csv("trade_data_processed_py.csv", index=False)



