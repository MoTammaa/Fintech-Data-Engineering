import M1
import pandas as pd
import main
import db


LOOKUP_TABLENAME = 'lookup_fintech_data_MET_P02_52_20136'
DATA_TABLENAME = 'fintech_data_MET_P02_52_20136_clean'
DATA_DIR = '././data/'

def clean(df : pd.DataFrame) -> pd.DataFrame:
    """
    This function cleans the data.
    It calls other functions in this file.
    """
    tidy = tidy_up(df)
    no_outliers = handling_outliers(tidy)
    imputed = imputation(no_outliers)
    transformed = transformation(imputed)
    return transformed


def handling_outliers(df : pd.DataFrame) -> pd.DataFrame:
    """
    This function handles the outliers in the data.
    It calls functions from the M1.
    """
    df_cleaned = M1.remove_outliers_log(df, M1.outliers_cols)

    return df_cleaned

def imputation(df : pd.DataFrame) -> pd.DataFrame:
    """
    This function imputes the missing values in the data.
    It calls functions from the M1.
    """
    df_imputed = M1.fillna_emp_title(df)
    df_imputed = M1.fillna_int_rate(df_imputed, 'mean')
    df_imputed = M1.fillna_annual_inc_joint(df_imputed)
    df_imputed = M1.fillna_emp_length(df_imputed)
    df_imputed = M1.fillna_home_ownership(df_imputed)

    return df_imputed

def transformation(df : pd.DataFrame) -> pd.DataFrame:
    """
    This function transforms the data.
    It calls functions from the M1.
    """
    df_transformed = M1.add_new_features(df)
    df_transformed = M1.encode_categorical(df_transformed, M1.encoding_candidate_columns)
    df_transformed = M1.normalize_columns(df_transformed)

    return df_transformed

def tidy_up(df : pd.DataFrame) -> pd.DataFrame:
    """
    This function tidies up the data (e.g column names, case sensitivity, etc).
    It calls functions from the M1.
    """
    df_tidied = M1.tidy_column_names(df)
    df_tidied = M1.tidy_issue_date(df_tidied)
    df_tidied = M1.tidy_term(df_tidied)
    # fix the case sensitivity of the data
    df_tidied = M1.clean_case_sensitivity(df_tidied)
    df_tidied = M1.tidy_type(df_tidied)

    return df_tidied

def get_cleaned_dataset() -> pd.DataFrame:
    """
    This function reads the dataset from the parquet file and cleans it then returns the cleaned dataset.
    """
    df = M1.read_parquet_file(f"{main.DATA_DIR}fintech_data.parquet")
    cleaned = clean(df)
    # M1.show_missing_values_stats(cleaned, True)
    # M1.save_cleaned_dataset_to_parquet(cleaned, "testt", main.DATA_DIR)
    return cleaned
   


def get_and_save_data():
    import os, pandas as pd
    if os.path.exists(f"{DATA_DIR}{DATA_TABLENAME}.parquet") and os.path.exists(f"{DATA_DIR}lookup_table.csv"):
        print("Reading data from parquet file. Clean Data already exists.")
        df = M1.read_parquet_file(f"{DATA_DIR}{DATA_TABLENAME}.parquet")
        lookup = pd.read_csv(f"{DATA_DIR}lookup_table.csv")
    else:
        print("Running the cleaning pipeline. Because ONE OR BOTH of the clean data or lookup data does not exist.")
        df = get_cleaned_dataset()
        M1.save_cleaned_dataset_to_parquet(df, DATA_TABLENAME, DATA_DIR)
        M1.save_lookup_table(M1.lookup_table, DATA_DIR)
        lookup = M1.lookup_table
    db.save_to_db(df, False, DATA_TABLENAME)
    db.save_to_db(lookup, False, LOOKUP_TABLENAME)
    return df


if __name__ == '__main__':
    get_cleaned_dataset()