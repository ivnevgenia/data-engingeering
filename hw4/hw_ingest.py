import duckdb
import requests
from pathlib import Path


BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv"

def download_and_convert_files():
    data_dir = Path("data") / "fhv"
    data_dir.mkdir(exist_ok=True, parents=True)

    year = 2019
    for month in range(1, 13):
        filename = f"fhv_tripdata_{year}-{month:02d}.csv.gz"
        filepath = data_dir / filename

        if filepath.exists():
            print(f"Пропуск {filename} (уже существует)")
            continue

        url = f"{BASE_URL}/{filename}"

        print(f"Загрузка {filename} из {url}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()


        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"Конвертация {filename} в Parquet...")
        con = duckdb.connect()
        con.execute(f"""
            COPY (SELECT * FROM read_csv_auto('{filepath}'))
            TO 'data/fhv/fhv_tripdata_{year}-{month:02d}.parquet' (FORMAT PARQUET)
        """)
        con.close()

        print(f"Файл {filename} успешно обработан.")

def update_gitignore():
    gitignore_path = Path(".gitignore")
    content = gitignore_path.read_text() if gitignore_path.exists() else ""

    if 'data/' not in content:
        with open(gitignore_path, 'a') as f:
            f.write('\n# Data directory\n/data/\n' if content else '# Data directory\n/data/\n')

if __name__ == "__main__":

    update_gitignore()

    download_and_convert_files()

    con = duckdb.connect("taxi_rides_ny.duckdb")
    con.execute("CREATE SCHEMA IF NOT EXISTS prod")


    con.execute("""
        CREATE OR REPLACE TABLE prod.fhv_tripdata AS
        SELECT * FROM read_parquet('data/fhv/*.parquet', union_by_name=true)
    """)


    con.close()
