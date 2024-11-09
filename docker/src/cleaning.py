import M1

def clean(df):
    """
    This function cleans the data.
    It calls other functions in this file.
    """
    no_outliers = handling_outliers(df)
    imputed = imputation(no_outliers)
    transformed = transformation(imputed)
    return transformed



def handling_outliers(df):
    """
    This function handles the outliers in the data.
    It calls functions from the M1.
    """
    pass

def imputation(df):
    """
    This function imputes the missing values in the data.
    It calls functions from the M1.
    """
    pass

def transformation(df):
    """
    This function transforms the data.
    It calls functions from the M1.
    """
    pass

def tidy_up(df):
    """
    This function tidies up the data.
    It calls functions from the M1.
    """
    pass