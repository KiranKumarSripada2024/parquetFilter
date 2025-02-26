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
def filter_parquet_files_by_date(df, date_str, folder_name):
    try:
        column_name = "event_time" if folder_name == "view_events" else "edited_date"

        if column_name not in df.columns:
            if folder_name != "view_events":
                return pl.DataFrame(), f"Missing '{column_name}' column"
            else:
                return pl.DataFrame(), None

        # Check if the column is already datetime
        if df[column_name].dtype != pl.Datetime:
            df = df.with_columns(
                pl.col(column_name)
                .str.to_datetime(format="%Y-%m-%d %H:%M:%S%.f", strict=False)
            )

        # Convert datetime to string format YYYY-MM-DD
        df = df.with_columns(
            pl.col(column_name)
            .dt.strftime("%Y-%m-%d")
            .alias("filtered_date")
        )

        df_filtered = df.filter(pl.col("filtered_date") == date_str)

        return df_filtered, None

    except Exception as e:
        return pl.DataFrame(), str(e)


# Process ZIP file
def process_parquet_files(zip_path, json_dir, error_log_path):
    files = extract_parquet_from_zip(zip_path)
    errors = {}
    folder_data = {}

    for folder, file_list in files.items():
        folder_filtered_data = []
        total_filtered_rows = 0
        file_details = []  # Stores file-specific row counts

        for file_name, file_content in file_list:
            try:
                df = pl.read_parquet(file_content)
                df_filtered, error = filter_parquet_files_by_date(df, date1, folder)

                if error and folder != "view_events":
                    errors.setdefault(folder, []).append({"file": file_name, "error": error})
                    continue

                row_count = df_filtered.height
                total_filtered_rows += row_count

                if row_count > 0:
                    if "filtered_date" in df_filtered.columns:
                        df_filtered = df_filtered.drop("filtered_date")
                    file_details.append({
                        "file": file_name,
                        "row_count": row_count
                    })

                    folder_filtered_data.extend([
                        {
                            **{k: (v.isoformat() if isinstance(v, (datetime, date)) else v) for k, v in row.items()}
                        }
                        for row in df_filtered.to_dicts()
                    ])
            except Exception as e:
                errors.setdefault(folder, []).append({"file": file_name, "error": str(e)})

        if folder_filtered_data:
            folder_data[folder] = {
                "data": folder_filtered_data,
                "controls": {
                    "total_filtered_rows": total_filtered_rows,
                    "edited_date": date1,
                    "files": file_details  # Add per-file row counts
                }
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
