import M1
import pandas as pd
import main
import db


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

def clean_row(row : pd.DataFrame) -> pd.DataFrame:
    """
    This function cleans a single row of data.
    It calls functions from the M1 as well as the database.
    """
    row_cleaned = tidy_up(row)
    row_cleaned = M1.remove_outliers_log(row_cleaned, M1.outliers_cols) # doesn't matter if it's a single row. The log transformation will be the same.
    row_cleaned = row_impute(row_cleaned)

    row_cleaned = M1.add_new_features(row_cleaned)
    # row_cleaned = M1.encode_
    row_cleaned = row_label_encode(row_cleaned)
    row_cleaned = row_OneHotEncode(row_cleaned)
    row_cleaned = M1.normalize_columns(row_cleaned)

    return row_cleaned


# dictionary to map the column names to the coumn name and value
enccolumn__column_value_dict = {
    'loan_status_late_(31_120_days)': ('loan_status', 'late_(31_120_days)'),
    'type_joint': ('type', 'joint'),
    'type_individual': ('type', 'individual'),
    'loan_status_current': ('loan_status', 'current'),
    'loan_status_default': ('loan_status', 'default'),
    'loan_status_fully_paid': ('loan_status', 'fully_paid'),
    'loan_status_in_grace_period': ('loan_status', 'in_grace_period'),
    'loan_status_late_(16_30_days)': ('loan_status', 'late_(16_30_days)'),
    'home_ownership_other': ('home_ownership', 'other'),
    'home_ownership_own': ('home_ownership', 'own'),
    'home_ownership_rent': ('home_ownership', 'rent'),
    'verification_status_source_verified': ('verification_status', 'source_verified'),
    'verification_status_verified': ('verification_status', 'verified'),
    'purpose_credit_card': ('purpose', 'credit_card'),
    'purpose_debt_consolidation': ('purpose', 'debt_consolidation'),
    'purpose_home_improvement': ('purpose', 'home_improvement'),
    'purpose_house': ('purpose', 'house'),
    'purpose_major_purchase': ('purpose', 'major_purchase'),
    'purpose_medical': ('purpose', 'medical'),
    'purpose_moving': ('purpose', 'moving'),
    'purpose_other': ('purpose', 'other'),
    'purpose_renewable_energy': ('purpose', 'renewable_energy'),
    'purpose_small_business': ('purpose', 'small_business'),
    'purpose_vacation': ('purpose', 'vacation'),
    'purpose_wedding': ('purpose', 'wedding')
}



def row_OneHotEncode(row : pd.DataFrame) -> pd.DataFrame:
    candidate_onehot_cols = list(filter(lambda x: x not in row.columns and x in enccolumn__column_value_dict \
                                        , db.get_columns_from_db(main.DATA_TABLENAME)))
    print("candidate_onehot_cols: ", candidate_onehot_cols)
    for index, r in row.iterrows():
        for col in candidate_onehot_cols:
            # add new hot encoded columns
            if col in enccolumn__column_value_dict:
                column_name, value = enccolumn__column_value_dict[col]
                row[col] = True if str(r[column_name]).replace(' ', '_').replace('-', '_').lower() == value else False
    return row




def row_impute(row : pd.DataFrame) -> pd.DataFrame:
    candidate_impute_cols = ['emp_title', 'int_rate', 'annual_inc_joint', 'emp_length', 'home_ownership', 'state', 'addr_state', 'letter_grade']
    for index, r in row.iterrows():
        for col in candidate_impute_cols:
            enterif = (col in row.columns)
            if enterif:
                enterif = (enterif and pd.isnull(r[col]))

            if enterif:
                val = db.get_imputation_from_db(r, main.DATA_TABLENAME, col)
                # print(f"---setting row[{col}]: {val} instead of {r[col]}")
                row[col] = val

    # convert emp_length to int
    row['emp_length'] = row['emp_length'].str.extract(r'(\d+)').astype(int)
    return row

def row_label_encode(row : pd.DataFrame) -> pd.DataFrame:
    candidate_labelenc_cols = ['emp_title', 'int_rate', 'annual_inc_joint', 'emp_length', 'home_ownership', 'state', 'addr_state', 'letter_grade']
    leave_same_name = ['emp_length', 'home_ownership', 'grade']
    for index, r in row.iterrows():
        for col in candidate_labelenc_cols:
            enterif = (col in row.columns)
            if enterif:
                enterif = (enterif and pd.isnull(r[col]))

            if not enterif: # label encoding
                val = db.impute_by_lookup_table(r, main.LOOKUP_TABLENAME, col)
                print(f"---label encoding row[{col}]: {r[col]} to {val}")
                if val is not None:
                    if col in leave_same_name:
                        row[col] = val
                        # print(f"---row[{col}]: {val}")
                    else:
                        row[f"{col}_encoded"] = val
    return row

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
   

if __name__ == '__main__':
    get_cleaned_dataset()