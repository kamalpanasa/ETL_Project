import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("Missing Supabase Url and Supabase Key in .env")
    return create_client(url, key)

def create_table_if_not_exists():
    try:
        supabase = get_supabase_client()
        create_table_sql = """
                id BIGSERIAL PRIMARY KEY,
                survived INTEGER,
                pclass INTEGER,
                sex TEXT,
                age FLOAT,
                sibsp INTEGER,
                parch INTEGER,
                fare FLOAT,
                embarked TEXT,
                class TEXT,
                who TEXT,
                adult_male BOOLEAN,
                deck TEXT,
                embark_town TEXT,
                alive TEXT,
                alone BOOLEAN,
                family_size INTEGER,
                is_alone INTEGER,
                age_group TEXT
            );
        """
        try:
            supabase.rpc('execute_sql', {'query': create_table_sql}).execute()
            print("Table titanic_data created")
        except Exception as e:
            print(f"Note: {e}")
            print("Table must be created manually or via migration")
    except Exception as e:
        print(f"Error creating table: {e}")
        print("Continuing with insertion...")

def load_to_supabase(staged_path: str, table_name: str = "titanic_data"):
    if not os.path.isabs(staged_path):
        staged_path = os.path.abspath(os.path.join(os.path.dirname(__file__), staged_path))
        print(f"Looking for the data file at: {staged_path}")

    if not os.path.exists(staged_path):
        print(f"Error: file not found at {staged_path}")
        print("Return to transform_titanic.py")
        return

    try:
        supabase = get_supabase_client()
        df = pd.read_csv(staged_path)
        total_rows = len(df)
        batch_size = 50
        print(f"Loading {total_rows} rows into '{table_name}'...")

        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i + batch_size].copy()
            batch = batch.where(pd.notnull(batch), None)
            records = batch.to_dict("records")

            try:
                response = supabase.table(table_name).insert(records).execute()
                end = min(i + batch_size, total_rows)
                print(f"Inserted {i+1}–{end} of {total_rows}")
            except Exception as e:
                print(f"Error in batch {i//batch_size + 1}: {str(e)}")
                continue

        print(f"Finished loading Titanic data into '{table_name}'")
    except Exception as e:
        print(f"Error loading data: {e}")

if __name__ == "__main__":
    staged_csv_path = os.path.join(os.path.dirname(__file__), "..", "data", "staged", "clean_titanic.csv")
    staged_csv_path = os.path.abspath(staged_csv_path)
    create_table_if_not_exists()
    load_to_supabase(staged_csv_path)