#importing 
import os
import seaborn as sns
import pandas as pd
from extract_iris import extract_data

def tranform_data(raw_path):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir = os.path.join(base_dir, "data", "staged")

    os.makedirs(staged_dir, exist_ok= True)
    df = pd.read_csv(raw_path)

    #1.Handle Missing Values

    numeric_cols = ["age", "fare"]
    
    #filling the missing values with median
    for col in numeric_cols:
        df[col] = df[col].fillna(df[col].median())
    
    df["embarked"] = df["embarked"].fillna(df["embarked"].mode()[0])

    #2.Feature engineering

    # IsAlone flag
    df["family_size"] = df["sibsp"] + df["parch"] + 1
    df["is_alone"] = (df["family_size"] == 1).astype(int)

    # Age group bucketing
    df["age_group"] = pd.cut(
        df["age"],
        bins=[0, 12, 18, 35, 60, 100],
        labels=["child", "teen", "youngadult", "adult", "senior"]
    )

    #3.Drop unnecessary columns

    df.drop(columns=["ticket"], inplace=True, errors="ignore")
    
    #4. saved Data
    staged_path = os.path.join(staged_dir, "clean_titanic.csv")
    df.to_csv(staged_path, index = False)

    print(f"Titanic data transformed and saved at: {staged_path}")
    return staged_path

if __name__ == "__main__":
    from extract_titanic import extract_titanic
    raw_path = extract_titanic()
    tranform_data(raw_path)