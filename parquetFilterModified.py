import zipfile
from datetime import datetime, timedelta, date
import polars as pl
import json
import requests
from io import BytesIO
import os
import configparser

# Load credentials from properties file
config = configparser.ConfigParser()
config.read("config.properties")

# Read values
username = config.get("DEFAULT", "USERNAME")
password = config.get("DEFAULT", "PASSWORD")

date1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

download_dir = "Zip_Download"
json_directory = "Json_filtered"
os.makedirs(download_dir, exist_ok=True)
os.makedirs(json_directory, exist_ok=True)

zip_path = os.path.join(download_dir, f"insights_{date1}.zip")
error_log_path = os.path.join(json_directory, "error.json")


# Function to download the ZIP file
def download_zip(snapshot_date, save_path):
    url = f"https://tdgroup-dev.collibra.com/rest/2.0/reporting/insights/directDownload?snapshotDate={snapshot_date}&format=zip"
    response = requests.get(url, auth=(username, password), stream=True)

    if response.status_code == 200:
        with open(save_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded: {save_path}")
    else:
        print(f"Failed to download {snapshot_date}: {response.status_code} - {response.text}")
        exit(1)


download_zip(date1, zip_path)


# Function to extract Parquet files from ZIP
def extract_parquet_from_zip(zip_path):
    extracted_files = {}
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            if file_name.endswith(".parquet"):
                folder = file_name.split("/")[0]  # Extract folder name
                if folder not in extracted_files:
                    extracted_files[folder] = []
                extracted_files[folder].append((file_name, BytesIO(zip_ref.read(file_name))))
    return extracted_files


# Function to filter rows where edited_date matches SYSDATE-1
def filter_parquet_files_by_date(df, date_str):
    try:
        if "edited_date" not in df.columns:
            return pl.DataFrame(), "Missing 'edited_date' column"

        df = df.with_columns(
            pl.col("edited_date")
            .cast(pl.Datetime("ms"))
            .dt.strftime("%Y-%m-%d")
            .alias("edited_date_as_date")
        )

        df_filtered = df.filter(pl.col("edited_date_as_date") == date_str)
        return df_filtered, None
    except Exception as e:
        return None, str(e)


# Process ZIP file
def process_parquet_files(zip_path, json_dir, error_log_path):
    files = extract_parquet_from_zip(zip_path)
    errors = {}
    folder_data = {}

    for folder, file_list in files.items():
        folder_filtered_data = []
        total_filtered_rows = 0

        for file_name, file_content in file_list:
            try:
                df = pl.read_parquet(file_content)
                df_filtered, error = filter_parquet_files_by_date(df, date1)

                if error:
                    errors.setdefault(folder, []).append({"file": file_name, "error": error})
                    continue

                row_count = df_filtered.height
                total_filtered_rows += row_count

                if row_count > 0:
                    folder_filtered_data.extend([
                        {k: (v.isoformat() if isinstance(v, (datetime, date)) else v) for k, v in row.items()}
                        for row in df_filtered.to_dicts()
                    ])
            except Exception as e:
                errors.setdefault(folder, []).append({"file": file_name, "error": str(e)})

        if folder_filtered_data:
            folder_data[folder] = {
                "data": folder_filtered_data,
                "controls": [{"total_filtered_rows": total_filtered_rows}]
            }

    # Save JSON files per folder
    for folder, data in folder_data.items():
        json_output_file = os.path.join(json_dir, f"{folder}-{date1}.json")
        with open(json_output_file, "w") as f:
            json.dump(data, f, indent=4)
        print(f"Filtered data for '{folder}' saved in {json_output_file}")

    # Save errors if any
    if errors:
        with open(error_log_path, "w") as f:
            json.dump({"errors": errors}, f, indent=4)
        print(f"Errors saved into {error_log_path}")


# Call the function
process_parquet_files(zip_path, json_directory, error_log_path)

print("Filtered edited_date data successfully saved in JSON.")