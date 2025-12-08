import os
import pandas as pd
from extract_iris import extract_data

def transform_data(raw_path):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir = os.path.join(base_dir,"data","staged")
    os.makedirs(staged_dir,exist_ok = True)
    df = pd.read_csv(raw_path)

    # Handle Missing Values

    numeric_cols = ["sepal_length","sepal_width","petal_length","petal_width"]

    # Filling the missing values with median

    for col in numeric_cols :
        df[col] = df[col].fillna(df[col].median())
        df["species"] = df["species"].fillna(df["species"].mode()[0])

    # Feature Engineering 
    df["sepal_ratio"] = df["sepal_length"]/df["sepal_width"]
    df["petal_ratio"] = df["petal_length"]/df["petal_width"]
    df["is_petal_long"] = (df["petal_length"]>df["petal_length"].median()).astype(int)

    # Drop unnecessary Columns 
    df.drop(columns = [],inplace = True, errors = "ignore")

    #Saved data
    staged_path = os.path.join(staged_dir,"iris_transformed.csv")
    df.to_csv(staged_path, index = False)

    print(f"Data transformed and saved at : {staged_path}")
    return staged_path

if __name__ == "__main__" :
    from extract_iris import extract_data
    raw_path = extract_data()
    transform_data(raw_path)