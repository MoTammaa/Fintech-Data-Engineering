import M1
import pandas as pd
import db


LOOKUP_TABLENAME = 'lookup_fintech_data_MET_P02_52_20136'
DATA_TABLENAME = 'fintech_data_MET_P02_52_20136_clean'
DATA_DIR = '././data/'
QUESTIONS = ['1. What is the distribution of loan amounts across different grades? (use letter grades or encoded grades (1-7) not grades from the uncleaned file)',
             '2. How does the loan amount relate to annual income across states ? (Interactive)',
             '3. What is the trend of loan issuance over the months (number of loans per month), filtered by year? (Interactive)',
            '4. Which states have the highest average loan amount?',
            '5. What is the percentage distribution of loan grades in the dataset?']


def extract_cleaned() -> pd.DataFrame:
    """
    This function reads the dataset from the parquet file and cleans it then returns the cleaned dataset.
    """
    # load data from parquet file
    df, lookup = read_data('extract_cleaned')
    cleaned = clean(df)

    # save the cleaned data & lookup to parquet file
    save_data(cleaned, M1.lookup_table, 'extract_cleaned')

    return cleaned

def transform() -> pd.DataFrame:
    """
    This function reads the dataset from the parquet file, transforms it then returns the transformed dataset.
    """
    # load data from parquet file
    df, lookup = read_data('transform')
    M1.lookup_table = lookup

    transformed = transformation(df)

    # save the cleaned data & lookup to parquet file
    save_data(transformed, M1.lookup_table, 'transform')

    return transformed
   

def clean(df : pd.DataFrame) -> pd.DataFrame:
    """
    This function cleans the data.
    It calls other functions in this file.
    """

    tidy = tidy_up(df)
    no_outliers = handling_outliers(tidy)
    imputed = imputation(no_outliers)
    return imputed


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

def transformation(df : pd.DataFrame) -> pd.DataFrame:
    """
    This function transforms the data.
    It calls functions from the M1.
    """
    df_transformed = M1.add_new_features(df)
    df_transformed = M1.encode_categorical(df_transformed, M1.encoding_candidate_columns)
    df_transformed = M1.normalize_columns(df_transformed)

    return df_transformed

def read_data(current_step: str) -> tuple:
    if 'transform' in current_step:
        return M1.read_parquet_file(f"{DATA_DIR}{DATA_TABLENAME}_cleaning_output.parquet"), M1.read_csv_file(f"{DATA_DIR}lookup_table_cleaning_output.csv")
    if 'load' in current_step or 'dash' in current_step:
        return M1.read_parquet_file(f"{DATA_DIR}{DATA_TABLENAME}_transformation_output.parquet"), M1.read_csv_file(f"{DATA_DIR}lookup_table_transformation_output.csv")
    # if 'clean' in current_step:
    return M1.read_parquet_file(f"{DATA_DIR}fintech_data.parquet"), None

def save_data(df: pd.DataFrame, lookup: pd.DataFrame, current_step: str):
    if 'clean' in current_step:
        M1.save_cleaned_dataset_to_parquet(df, DATA_TABLENAME + '_cleaning_output', DATA_DIR)
        M1.save_lookup_table(lookup, DATA_DIR, 'lookup_table_cleaning_output')
    elif 'transform' in current_step:
        M1.save_cleaned_dataset_to_parquet(df, DATA_TABLENAME + '_transformation_output', DATA_DIR)
        M1.save_lookup_table(lookup, DATA_DIR, 'lookup_table_transformation_output')

def load_to_db():
    import os, pandas as pd
    if not (os.path.exists(f"{DATA_DIR}{DATA_TABLENAME}_transformation_output.parquet") and os.path.exists(f"{DATA_DIR}lookup_table_transformation_output.csv")):
        raise Exception("Clean Data does not exist. Please run the whole pipeline first")
    
    df, lookup = read_data('load_to_db')
    print('Successfully loaded files')

    db.save_to_db(df, False, DATA_TABLENAME)
    db.save_to_db(lookup, False, LOOKUP_TABLENAME)
    print('Successfully saved to database')
    return df


if __name__ == '__main__':
    extract_cleaned()
    transform()
    load_to_db()
