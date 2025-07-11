import os
import shutil
import csv
from datetime import datetime

def backup_csv_if_exists(csv_path):
    """
    If the CSV file exists, create a backup with a timestamp.
    """
    if os.path.exists(csv_path):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = csv_path.replace(".csv", f".backup_{timestamp}.csv")
        shutil.copy2(csv_path, backup_path)
        print(f"Backup created: {backup_path}")

def append_row_to_csv(csv_path, row, header):
    """
    Append a row (dict) to the CSV file, writing the header if the file does not exist.
    """
    file_exists = os.path.exists(csv_path)
    with open(csv_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row) 