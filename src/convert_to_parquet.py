# convert_to_parquet.py
# Create actual Parquet file for Databricks
import json
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

print("Reading JSON files from 2024 folder...")
json_dir = Path("2024")
data = []

count = 0
for json_file in json_dir.rglob("*.json"):
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
            # Convert to JSON string to avoid type conflicts
            data.append({'json_data': json.dumps(json_data)})
            count += 1
            if count % 5000 == 0:
                print(f"Read {count} files...")
    except Exception as e:
        print(f"Error reading {json_file}: {e}")

print(f"Read {len(data)} JSON files")

# Convert to PyArrow Table
print("Converting to PyArrow Table...")
table = pa.Table.from_pylist(data)

# Create output directory
output_dir = Path("cve_2024_parquet")
output_dir.mkdir(exist_ok=True)

# Write Parquet file
print("Writing to Parquet...")
pq.write_table(
    table,
    output_dir / "2024_parquet.parquet",
    compression='snappy'
)

# Create _SUCCESS file
(output_dir / "_SUCCESS").touch()

file_size = (output_dir / "2024_parquet.parquet").stat().st_size / (1024*1024)
print(f"\nCreated Parquet with {len(data)} records")
print(f"Output: cve_2024_parquet/2024_parquet.parquet")
print(f"File size: {file_size:.2f} MB")
print("\nUpload the entire 'cve_2024_parquet' folder to Databricks")