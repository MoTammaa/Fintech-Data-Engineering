# %% [markdown]
# # 0- Importing Libraries and Loading Data

# %%
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import pyarrow as pa

# %%
# modify by placing your own directory where the datasets resides
data_dir = './Datasets/'

# %% [markdown]
# # 1- Extraction

# %% [markdown]
# Read the csv file and convert it to parquet format (first time only).

# %%
# read the csv file and convert it to parquet
# fintech_df_csv = pd.read_csv(data_dir + 'fintech_data_38_52_20136.csv')
# pq.write_table(pa.Table.from_pandas(fintech_df_csv), data_dir + 'fintech_data.parquet')
pd.set_option('display.max_columns', None)

# %% [markdown]
# Initialize variables.

# %%
fintech_df_uncleaned : pd.DataFrame = None
fintech_df : pd.DataFrame = None

lookup_table : pd.DataFrame = pd.DataFrame(columns=['column', 'original', 'imputed'])


# %% [markdown]
# read the parquet file normally and show the first row just to make sure that the data is loaded correctly.

# %%
def read_parquet_file(file_path=data_dir + 'fintech_data.parquet') -> pd.DataFrame:
    global fintech_df_uncleaned, data_dir
    fintech_df_uncleaned = pq.read_table(file_path).to_pandas().set_index('Loan Id')
    return fintech_df_uncleaned
read_parquet_file().sample(2)

# %% [markdown]
# # 2- EDA

# %% [markdown]
# - First, we need to check the columns and their data types *(i.e. some info)*.

# %%
fintech_df_uncleaned.info()

# %% [markdown]
# - Second, we may need to check the percentage of missing values in each column.

# %%
# form output for each column get missing values count and percentage
def show_missing_values_stats(df: pd.DataFrame, hide_non_missing: bool = False):
    missing_values = pd.DataFrame(df.isnull().sum(), columns=['Missing Values'])
    missing_values['Percentage'] = missing_values['Missing Values'] / df.shape[0] * 100
    missing_values['Data Type'] = df.dtypes
    missing_values['Unique'] = df.nunique()
    if hide_non_missing:
        missing_values = missing_values[missing_values['Missing Values'] > 0]
    missing_values = missing_values.sort_values(by=['Missing Values', 'Unique'], ascending=False)
    print(missing_values)
show_missing_values_stats(fintech_df_uncleaned)

# %%
fintech_df_uncleaned.describe()

# %%
# plot the distribution of all numeric columns in one plot 4x2
def plot_distribution(df: pd.DataFrame):
    fig, axes = plt.subplots(4, 2, figsize=(20, 20))
    for i, col in enumerate(df.select_dtypes(include=np.number).columns):
        sns.histplot(df[col], ax=axes[i // 2, i % 2], kde=True, color='blue')
        axes[i // 2, i % 2].set_title(f'Distribution of {col}')
    plt.show()
plot_distribution(fintech_df_uncleaned)


# %% [markdown]
# - From the previous steps graphs, we can see that the values of Annual Income(/Joint) & (Tot/Avg) Cur Bal are hugely skewed to the right. They appear also to have lots of far outliers.

# %% [markdown]
# - The following data description and the correlation graphs can be used to understand the data more, and maybe impute the missing values with highly correlated features.

# %%
fintech_df_uncleaned.describe(include=['object'])

# %%
fintech_df_uncleaned.corr(numeric_only=True)

# %%
def plot_correlation_heatmap(df: pd.DataFrame):
    plt.figure(figsize=(12, 10))
    sns.heatmap(df.corr(numeric_only=True), annot=True, cmap='coolwarm', fmt='.2f', linewidths=2)
    plt.show()
plot_correlation_heatmap(fintech_df_uncleaned)

# %% [markdown]
# # 3 - Cleaning Data

# %% [markdown]
# ## Tidying up column names

# %%
# tidy up column names by removing spaces and converting to lower case
fintech_df = fintech_df_uncleaned.copy()
fintech_df.columns = fintech_df_uncleaned.columns.str.replace(' ', '_').str.lower()

# %%
fintech_df.columns

# %% [markdown]
# ## Converting data types

# %% [markdown]
# ### Convert emp_length to integer

# %%
fintech_df['emp_length'].value_counts()

# %% [markdown]
# - we can choose to change 10+ years to 10 and < 1 year to 0 to make it easier to work with.

# %% [markdown]
# ### Convert issue_date to datetime

# %%
# convert issue_date to datetime
def tidy_issue_date() -> pd.Series:
    return pd.to_datetime(fintech_df['issue_date'])
fintech_df['issue_date'] = tidy_issue_date()

# %% [markdown]
# ### Convert term to integer (months)

# %%
def tidy_term() -> pd.Series:
    return fintech_df['term'].astype(str).str.extract(r'(\d+)').astype(int)
fintech_df['term'] = tidy_term()
print(fintech_df['term'].value_counts())

# %% [markdown]
# ## Observe inconsistent data

# %%
for col in fintech_df_uncleaned.select_dtypes(include=np.object_).columns:
    # print the column name
    print(f'Column: {col}')
    print(fintech_df_uncleaned[col].unique())
    print('\n'+ ("-"*50) +'\n')

# %%
# try to find inconsistencies in the data like case sensitivity
fintech_df_2 = fintech_df_uncleaned.copy()

for col in fintech_df_2.select_dtypes(include=np.object_).columns:
    # Convert the column to uppercase
    fintech_df_2[col + 'upper'] = fintech_df_2[col].str.upper()

    # Group by the uppercase values and filter groups where original values differ
    inconsistencies = fintech_df_2.groupby(col + 'upper').filter(lambda x: len(x[col].unique()) > 1)

    if inconsistencies.shape[0] > 0:
        inc = inconsistencies[col].unique()
        print(f'Column: {col} -> {inc.shape[0]} inconsistencies')
        inc.sort()
        print(inc)

# %% [markdown]
# check duplicated rows or columns.

# %%
print('dup rows number: ',fintech_df_uncleaned[fintech_df_uncleaned.duplicated()].shape[0])
# check if there are duplicate columns
transposed_df = fintech_df_uncleaned.T

dup_cols = transposed_df[transposed_df.duplicated()]
# Get pairs of duplicate columns
duplicate_pairs = []
for dcol in dup_cols.index:
    for col in fintech_df_uncleaned.columns:
        if fintech_df_uncleaned[fintech_df_uncleaned[dcol] == fintech_df_uncleaned[col]].shape[0] == fintech_df_uncleaned.shape[0] and dcol != col:
                duplicate_pairs.append((dcol, col))

print(duplicate_pairs)


# %% [markdown]
# ## Findings and conclusions

# %% [markdown]
# emp_title, type and description columns contain data that varies in case only. We can convert them to lowercase.
# *(unique values came down from 821 to 686)*

# %%
def clean_case_sensitivity(df: pd.DataFrame) -> pd.DataFrame:
    fintech_df_cpy = df.copy()
    fintech_df_cpy['emp_title'] = fintech_df['emp_title'].str.lower()
    fintech_df_cpy['type'] = fintech_df_cpy['type'].str.lower()
    fintech_df_cpy['description'] = fintech_df_cpy['description'].str.lower()
    return fintech_df_cpy
fintech_df = clean_case_sensitivity(fintech_df)

# %% [markdown]
# We can also see that in `type` there is joint and joint app. We can convert them to joint.

# %%
fintech_df['type'].value_counts()

# %%
fintech_df['type'] = fintech_df['type'].replace({'joint app': 'joint'})
fintech_df['type'].value_counts()

# %% [markdown]
# - I also found that there are 2 pairs of columns that are duplicates. But, theoretically, they should differ. So maybe if we get more data, we can have different values in them. So let's keep them for now.

# %%


# %% [markdown]
# ## Observing outliers

# %%
outliers_cols = ['avg_cur_bal', 'tot_cur_bal', 'annual_inc', 'annual_inc_joint']# + ['loan_amount', 'int_rate', 'funded_amount', 'grade']

# %%
# plot possiblly outliers kde 
def plot_columns(df: pd.DataFrame, cols: list, lim_e6: float = 0.5):
    fig, axes = plt.subplots(int(len(cols)/2 + 0.5), 2, figsize=(20, 10 if len(cols) < 7 else 20))
    for i, col in enumerate(cols):
        sns.histplot(df[col], ax=axes[i // 2, i % 2], kde=True)
        axes[i // 2, i % 2].set_title(f'Plotting of {col}')
        # limit the x-axis to 0.5 * 10^6
        x_max = lim_e6 * 1000000 * (0.25 if col == 'avg_cur_bal' else 1)
        if lim_e6 > 0:
            axes[i // 2, i % 2].set_xlim(0, x_max)
    plt.show()

plot_columns(fintech_df, outliers_cols,0)


# %% [markdown]
# ### Plotting Z-scores for outliers:
# Reason for this:
# - *Data is Normally Distributed*: Z-scores are most effective when the data follows a normal distribution or is      approximately symmetric. In this case, the Z-score gives a good measure of how far each point is from the mean.
# 
# - *Data is Continuous*: Z-score is suitable when the data is continuous and you want to detect points far from the mean.

# %%
# plot and observe outliers zscore
def plot_columns_zscore(df: pd.DataFrame, cols: list):
    fig, axes = plt.subplots(int(len(cols)/2 +0.5), 2, figsize=(20, 15))
    for i, col in enumerate(cols):
        sns.histplot(np.abs((df[col] - df[col].mean()) / df[col].std()), ax=axes[i // 2, i % 2], kde=True)
    plt.show()

plot_columns_zscore(fintech_df, outliers_cols)

# %% [markdown]
# By plotting the Z-scores, we can see that there are many outliers in the data in columns -> `annual_inc`, `annual_inc_joint`, `tot_cur_bal` and `avg_cur_bal` specifically.

# %% [markdown]
# ## Handling outliers

# %% [markdown]
# #### Among the techniques to handle outliers, we can choose to apply **Log Transformation**:

# %%
def remove_outliers_log(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    fintech_df_cpy = df.copy()
    for col in cols:
        fintech_df_cpy[col] = np.log1p(fintech_df_cpy[col])
    return fintech_df_cpy

fintech_df_cleaned_log_outliers = remove_outliers_log(fintech_df, outliers_cols)
plot_columns_zscore(fintech_df_cleaned_log_outliers, outliers_cols)

# %%
plot_columns(fintech_df_cleaned_log_outliers, outliers_cols, 0)

# %% [markdown]
# The above was the Plotting of data after applying log transformation to the columns with outliers. We can see that the data is now more normally distributed, and the outliers are less visible. They also have a good zscores range.
# 
# We can now save the columns

# %%
for col in outliers_cols:
    fintech_df[col] = fintech_df_cleaned_log_outliers[col]

# %% [markdown]
# ###### I tried log transformation on int_rate, but it didn't work well. Nearly had no effect on the data. 

# %% [markdown]
# ## Observing Missing Data

# %%
show_missing_values_stats(fintech_df, hide_non_missing=True)

# %% [markdown]
# #### 1. For `emp_title` (8.81%), we can see that:
# - around 21% of the rows that are missing `emp_title` have a value in `emp_length`
# - all people that have missing values in `emp_title` have a value in `annual_inc`. As `annual_inc` have no missing values.
# - some people have missing values in `emp_title` but have a value in `emp_length`. And some have missing values in `emp_length` but have a value in `emp_title`. Therefore, it can be concluded that this data is maybe missing at random.
# 

# %%
fintech_df[fintech_df['emp_title'].isnull() & fintech_df['emp_length'].notnull()].shape[0] / fintech_df[fintech_df['emp_title'].isnull()].shape[0] * 100,                                                                                                                        fintech_df[fintech_df['emp_title'].isnull() & fintech_df['annual_inc'].isnull()].shape[0] / fintech_df[fintech_df['emp_title'].isnull()].shape[0] * 100


# %% [markdown]
# #### 2. For `int_rate` (4.68%):
# - we cannot say that the missing values mean that the loan has zero interest rate as this is not realistic (at least in the US, where the data is based), so maybe it is missing completely at random.
# - we need to impute the missing values.
# - we can see from the correlation matrix that `int_rate` is highly correlated with `grade` (92%). So we can use `grade` to impute the missing values.
# 
#     

# %%
fintech_df[fintech_df['int_rate'].isnull()]['loan_status'].value_counts()
# plot_correlation_heatmap(fintech_df)
fintech_df.corr(numeric_only=True)['int_rate'].sort_values(ascending=False)

# %% [markdown]
# #### 3. For `annual_inc_joint` (92.98%):
# - we can see that the `annual_inc_joint` is missing for all the rows or loans that are not of type `joint`. 
# - we can hence deduce that the missing values are MNAR (missing not at random) as they are related to the type of the loan.
# - we can deduce that the it is missing purposely as it is not applicable for non-joint loans.
# - since the `annual_inc_joint` is the combined income of the borrower and the co-borrower, we can impute the missing values with the value of `annual_inc` as we can say that the co-borrower has no income.

# %%
print('The types of loan (& their counts) that exist are:',fintech_df['type'].value_counts().to_dict(),'\n', ('-'*50))
print('The types of loan that exist which have annual_inc_joint null are:', fintech_df[fintech_df['annual_inc_joint'].isnull()]['type'].value_counts().to_dict(),'\n', ('-'*50))
print('Number of people who have type of loan = "joint" & have annual_inc_joint null =',  fintech_df[ (fintech_df['type'] == 'joint') & fintech_df['annual_inc_joint'].isnull()].shape[0])

# %% [markdown]
# #### 4. For `emp_length` (6.98%):
# - we can see that some of the rows that have missing values in `emp_length` have a value in `emp_title`. And some have missing values in `emp_title` but have a value in `emp_length`. Therefore, it can be concluded that this data is maybe missing at random.

# %%
print(fintech_df['emp_length'].isnull().sum() , fintech_df['emp_length'].value_counts().to_dict())
print(' The number of people who have emp_length null and emp_title null are:', fintech_df[ (fintech_df['emp_length'].isnull()) & (fintech_df['emp_title'].isnull())].shape[0])
print('VS number of people who have emp_length null and emp_title not null are:', fintech_df[ (fintech_df['emp_length'].isnull()) & (fintech_df['emp_title'].notnull())].shape[0])



# %% [markdown]
# #### 5. There is a value '**ANY**' in `home_ownership` that is not clear. It's a small percentage of the data.

# %%
fintech_df['home_ownership'].value_counts()

# %% [markdown]
# ## Handling Missing data

# %% [markdown]
# #### 1. For `emp_title`:
# - After observing the data, trying multi-variate imputation, and checking multiple variables' combinations (with preserving logical relationships of course), I found that the best way to impute the missing values is to use the `grade` and `home_ownership` columns:
#     - The `home_ownership` column can help us group, because sometimes ownerships, salaries, and job titles are related.
#     - The `grade` column can help us group, because sometimes the grade of the loan can be related to the job title, it could refer to the history of the borrower, or it could refer to the salary and the borrower's ability to pay.
# - The two columns chosen in grouping gave us the best results in imputing the missing values distribution-wise, rather than just increasing already high-dense values.

# %%
# # Calculate the mode for each group
# group_means = fintech_df.groupby('annual_inc')['emp_title'].apply(lambda x: x.mode().iloc[0] if not x.mode().empty else 'Unknown')
# group_means = fintech_df.groupby(['grade', 'annual_inc'])['emp_title'].apply(lambda x: x.mode().iloc[0] if not x.mode().empty else 'Unknown')
def fill_na_emp_title(df : pd.DataFrame, grouping_rows: list = ['grade', 'home_ownership']) -> pd.DataFrame:
    mode = df['emp_title'].mode().iloc[0]
    group_means = df.groupby(grouping_rows)['emp_title'].apply(lambda x: x.mode().iloc[0] if not x.mode().empty else mode)
    def filling_lambda_emp_title(row, group_means, grouping_rows):
        if pd.isna(row['emp_title']):
            key = tuple(row[grp] for grp in grouping_rows)
            if key in group_means:
                return group_means[key]
        return row['emp_title']

    # Apply the function
    df_cpy = df.copy()
    df_cpy['emp_title'] = df.apply(lambda row: filling_lambda_emp_title(row, group_means, grouping_rows), axis=1)

    # We will not update the lookup table with emp_title for now
    # # Update the lookup table
    # global lookup_table
    # missing_values = df['emp_title'].isnull()
    # new_lookup_entries = pd.DataFrame({
    #     'column': ['emp_title'] * missing_values.sum(),
    #     'original': df.loc[missing_values, 'emp_title'],
    #     'imputed': df_cpy.loc[missing_values, 'emp_title']
    # }).drop_duplicates(subset=['column', 'original', 'imputed'])
    
    # lookup_table = pd.concat([lookup_table, new_lookup_entries], ignore_index=True)
    
    return df_cpy

fintech_df = fill_na_emp_title(fintech_df)
fintech_df['emp_title'].isnull().sum()


# %%


# %% [markdown]
# #### 2. For `int_rate` we will use the `grade` column to impute the missing values. And we will check which of the median or mean is better to use. (reasoning above)

# %%
def int_rate_fillna(df: pd.DataFrame, meanormedian ='mean') -> pd.DataFrame:
    fintech_df_int_rate = df.copy()

    fintech_df_int_rate['int_rate'] = fintech_df_int_rate.groupby('grade')['int_rate'].transform(lambda x: x.fillna(x.median() if meanormedian == 'median' else x.mean()))

    # # Update the lookup table
    # global lookup_table
    # missing_values = df['int_rate'].isnull()
    # new_lookup_entries = pd.DataFrame({
    #     'column': ['int_rate'] * missing_values.sum(),
    #     'original': df.loc[missing_values, 'int_rate'],
    #     'imputed': fintech_df_int_rate.loc[missing_values, 'int_rate']
    # }).drop_duplicates(subset=['column', 'original', 'imputed'])

    # lookup_table = pd.concat([lookup_table, new_lookup_entries], ignore_index=True)
    return fintech_df_int_rate


# %%
fintech_df_int_rate_mean = int_rate_fillna(fintech_df, 'mean')
fintech_df_int_rate_median = int_rate_fillna(fintech_df, 'median')

fig, axes = plt.subplots(1, 2, figsize=(12, 5))

sns.histplot(fintech_df_int_rate_mean['int_rate'], ax=axes[0], kde=True, color='red')
sns.histplot(fintech_df['int_rate'], ax=axes[0], kde=True)
axes[0].set_title('Original VS Mean')

sns.histplot(fintech_df_int_rate_median['int_rate'], ax=axes[1], kde=True, color='red')
sns.histplot(fintech_df['int_rate'], ax=axes[1], kde=True)
axes[1].set_title('Original VS Median')

plt.tight_layout()
plt.show()


# %% [markdown]
# There may not be a noticiable difference between the two methods. So we can use the mean.

# %%
fintech_df['int_rate'] = int_rate_fillna(fintech_df)['int_rate']

lookup_table[lookup_table['column'] == 'int_rate']

# %% [markdown]
# #### 3. since the `annual_inc_joint` is the combined income of the borrower and the co-borrower, we can impute the missing values with the value of `annual_inc` as we can say that the co-borrower has no income.

# %%
def annual_inc_joint_fillna(df: pd.DataFrame) -> pd.DataFrame:
    fintech_df_annual_inc_joint = df.copy()

    fintech_df_annual_inc_joint['annual_inc_joint'] = fintech_df_annual_inc_joint['annual_inc_joint'].fillna(fintech_df_annual_inc_joint['annual_inc'])

    # Update the lookup table
    # global lookup_table
    # missing_values = df['annual_inc_joint'].isnull()
    # new_lookup_entries = pd.DataFrame({
    #     'column': ['annual_inc_joint'] * missing_values.sum(),
    #     'original': df.loc[missing_values, 'annual_inc_joint'],
    #     'imputed': fintech_df_annual_inc_joint.loc[missing_values, 'annual_inc_joint']
    # }).drop_duplicates(subset=['column', 'original', 'imputed'])

    # lookup_table = pd.concat([lookup_table, new_lookup_entries], ignore_index=True)
    
    return fintech_df_annual_inc_joint

# %%
fintech_df['annual_inc_joint'] = annual_inc_joint_fillna(fintech_df)['annual_inc_joint']
lookup_table[lookup_table['column'] == 'annual_inc_joint']

# %% [markdown]
# #### 4. For `emp_length`:
# - We can try to impute the missing values using the `emp_title` & `annual_inc` columns. As the job title and the salary can be related to the employment length and closely related to each other.
# - We can also convert the `emp_length` to integer value to make it easier to work with.
# - We can hence convert 10+ years to 10 and < 1 year to 0.

# %%

def emp_length_fillna(df: pd.DataFrame):
    fintech_df_emp_length = df.copy()
    mode = fintech_df_emp_length['emp_length'].mode()[0]
    fintech_df_emp_length['emp_length'] = fintech_df_emp_length.groupby(['emp_title', 'annual_inc'])['emp_length'].transform(lambda x: x.fillna(x.mode()[0] if x.mode().shape[0] > 0 else mode))
    # change the type to integer
    # but first we need to convert '< 1 year' to 0 and '10+ years' to 11
    fintech_df_emp_length['emp_length'] = fintech_df_emp_length['emp_length'].replace({'< 1 year': '0 years', '10+ years': '10 years'})
    fintech_df_emp_length['emp_length'] = fintech_df_emp_length['emp_length'].str.extract(r'(\d+)').astype(int)

    # Update the lookup table
    global lookup_table
    # Identify transformed values
    transformed_values = df['emp_length'].isin(['< 1 year', '10+ years'])

    # Create new lookup entries DataFrame
    new_lookup_entries = pd.DataFrame({
        'column': ['emp_length'] * transformed_values.sum(),
        'original': df.loc[transformed_values, 'emp_length'],
        'imputed': fintech_df_emp_length.loc[transformed_values, 'emp_length']
    }).drop_duplicates(subset=['column', 'original', 'imputed'])

    lookup_table = pd.concat([lookup_table, new_lookup_entries], ignore_index=True)
    
    
    return fintech_df_emp_length

# %%

fintech_df_emplength = emp_length_fillna(fintech_df)
print(fintech_df['emp_length'].value_counts(),fintech_df_emplength['emp_length'].value_counts())
# fintech_df_emplength.corr(numeric_only=True)['emp_length'].sort_values(ascending=False) * 100
# print(test[test['emp_length'].isnull()].shape[0])
# print(test[test['emp_length'] == 'Unknown'])
# print(test[ test['emp_title'].str.contains('basketball')]['emp_title'].value_counts())
# test[test['emp_title'].str.startswith('basketball')]
print(lookup_table[lookup_table['column'] == 'emp_length'])

# %%
fig, axes = plt.subplots(1, 1, figsize=(8, 4))
sns.histplot(fintech_df_emplength['emp_length'], kde=True)
axes.set_title(f'Distribution of emp_length after filling missing values')

plt.tight_layout()
plt.show()

# %%
fintech_df = fintech_df_emplength

# %% [markdown]
# That's the best I could do for the data of this column.

# %% [markdown]
# #### 5. For `home_ownership`:
# - We can see that there is a value '**ANY**' that is not clear. It's a small percentage of the data. 
# <!-- - We can group it by 'annual_inc' and get mode because sometimes ownerships and salaries are related. Also, it wouldn't make a huge difference. -->
# - We can make it 'OTHER' to unify the data. It wouldn't make a huge difference.
# 

# %%
def home_ownership_fillna(df: pd.DataFrame) -> pd.DataFrame:
    fintech_df_home_ownership = df.copy()
    fintech_df_home_ownership['home_ownership'] = fintech_df_home_ownership['home_ownership'].str.upper()
    # mode = fintech_df_home_ownership['home_ownership'].mode()[0]
    # fintech_df_home_ownership['home_ownership'] = fintech_df_home_ownership.groupby('annual_inc')['home_ownership'].transform(lambda x: x.replace('ANY', x.mode()[0] if x.mode().shape[0] > 0 and x.mode()[0] != 'ANY' else mode))
    # # Update the lookup table
    # global lookup_table
    # missing_values = df['home_ownership'].isnull()
    # transformed_values = df['home_ownership'] == 'ANY'
    # new_lookup_entries = pd.DataFrame({
    #     'column': ['home_ownership'] * (missing_values.sum() + transformed_values.sum()),
    #     'original': pd.concat([df.loc[missing_values, 'home_ownership'], df.loc[transformed_values, 'home_ownership']]),
    #     'imputed': pd.concat([fintech_df_home_ownership.loc[missing_values, 'home_ownership'], fintech_df_home_ownership.loc[transformed_values, 'home_ownership']])
    # }).drop_duplicates(subset=['column', 'original', 'imputed'])
    # lookup_table = pd.concat([lookup_table, new_lookup_entries], ignore_index=True)
    fintech_df_home_ownership['home_ownership'] = fintech_df_home_ownership['home_ownership'].replace('ANY', 'OTHER')

    global lookup_table
    # add (ANY, OTHER) to lookup table
    new_lookup_entry = pd.DataFrame({
        'column': ['home_ownership'],
        'original': ['ANY'],
        'imputed': ['OTHER']
    })
    lookup_table = pd.concat([lookup_table, new_lookup_entry], ignore_index=True)
    
    return fintech_df_home_ownership
    
fintech_df = home_ownership_fillna(fintech_df)
fintech_df['home_ownership'].value_counts(), lookup_table[lookup_table['column'] == 'home_ownership']

# %% [markdown]
# ## Findings and conclusions

# %%


# %% [markdown]
# # 4 - Data transformation and feature eng.

# %% [markdown]
# ## 4.1 - Adding Columns

# %% [markdown]
# #### 1. Adding a column for `month_number` : int,  from the `issue_date` column.
# #### 2. Adding a column for `salary_can_cover` : bool,  if the `annual_inc` is greater than the `loan_amount`.
# #### 3. Adding a column for `letter_grade` : str : categorical {A, B, C, D, E, F, G},  from the `grade` column.
# #### 4. Adding a column for `installment_per_month` : float, where monthly installment M =>
# $$M = P \times \frac{r \times (1 + r)^n}{(1 + r)^n - 1}$$
# where:
#  - M is the monthly installment
#  - P is the loan principle/amount
#  - r is the monthly interest rate , r = int rate/12
#  - n is the number of payments/months
# 
# 

# %% [markdown]
# ##### ***Note: The `annual_inc` column has been log transformed, so we need to revert it back to its original form before calculating the `salary_can_cover` column.***

# %%
def add_new_features(df: pd.DataFrame) -> pd.DataFrame:
    fintech_df_new = df.copy()
    # inverse log1 transformation the annual_inc
    annual_inc = np.expm1(fintech_df_new['annual_inc'])
    fintech_df_new['month_number'] = fintech_df['issue_date'].dt.month.astype(int)
    fintech_df_new['salary_can_cover'] = annual_inc >= fintech_df['loan_amount']
    def grade_to_letter(grade : int) -> str:
        return 'A' if grade <= 5 else 'B' if grade <= 10 else 'C' if grade <= 15 else 'D' if grade <= 20 else 'E' if grade <= 25 else 'F' if grade <= 30 else 'G'
    fintech_df_new['letter_grade'] = fintech_df['grade'].apply(grade_to_letter)
    fintech_df_new['installment_per_month'] = fintech_df['loan_amount'] * (fintech_df['int_rate'] / 12) *\
                ((1 + (fintech_df['int_rate'] / 12)) ** fintech_df['term'])\
                / (((1 + (fintech_df['int_rate'] / 12)) ** fintech_df['term']) - 1)
    
    # Update the lookup table with the grade to letter mapping
    global lookup_table
    new_rows = pd.DataFrame([{'column': 'grade', 'original': i, 'imputed': grade_to_letter(i)} for i in range(1, 36)])
    lookup_table = pd.concat([lookup_table, new_rows], ignore_index=True)
    
    return fintech_df_new

fintech_df = add_new_features(fintech_df)
print(lookup_table[lookup_table['column'] == 'grade'])
fintech_df.sample(3)

# %%


# %% [markdown]
# ## 4.2 - Encoding

# %% [markdown]
# #### Checking what columns need to be encoded.

# %%
print(list(filter(lambda c : fintech_df[c].value_counts().shape[0] < fintech_df.shape[0],fintech_df.select_dtypes(include=np.object_).columns)), '\n', ('-'*50))
encoding_candidate_columns = ['home_ownership', 'verification_status', 'purpose', 'type', 'loan_status', 'addr_state', 'state', 'letter_grade']
for col in encoding_candidate_columns:
    print(f'Column: {col}')
    print(fintech_df[col].unique())
    print(("-"*50))

# %% [markdown]
# #### Encoding Process:
# 1. Encoding the `state` & `addr_state` columns using label encoding. why? because they are categorical columns with a lot of unique values. It may add much overhead to the model if we use one-hot encoding.
# 2. Encoding the `*[home_ownership, verification_status, type, loan_status, purpose]*` columns using one-hot encoding. why? because they are categorical columns with few unique values, and they are not ordinal.
# 3. Encoding the `letter_grade` column using label encoding. why? because it is an ordinal column.

# %%
# lookup_table = pd.DataFrame(columns=['column', 'original', 'imputed'])

def encode_categorical(df: pd.DataFrame) -> pd.DataFrame:
    fintech_df_encoded = df.copy()
    label_encoding_columns = list(filter(lambda c : 'state' in c or c == 'letter_grade', encoding_candidate_columns))
    one_hot_encoding_columns = list(filter(lambda c : c not in label_encoding_columns, encoding_candidate_columns))
    for col in label_encoding_columns:
        fintech_df_encoded[col] = fintech_df_encoded[col].astype('category')
        fintech_df_encoded[col+'_encoded'] = fintech_df_encoded[col].cat.codes
    fintech_df_encoded = pd.get_dummies(fintech_df_encoded, columns=one_hot_encoding_columns, prefix=one_hot_encoding_columns, drop_first=True)
    fintech_df_encoded.columns = fintech_df_encoded.columns.str.replace(' ', '_').str.replace('-', '_').str.lower()

    # store the encoding in the lookup table (column, original, imputed)
    for col in label_encoding_columns:
        for unique_val in fintech_df_encoded[col].unique():
            new_row = {
                'column': col,
                'original': unique_val,
                'imputed': fintech_df_encoded[fintech_df_encoded[col] == unique_val][col + '_encoded'].iloc[0]
            }
            if new_row not in lookup_table.to_dict(orient='records'):
                lookup_table.loc[len(lookup_table)] = new_row
        
    return fintech_df_encoded

fintech_df_encoded = encode_categorical(fintech_df)
print(lookup_table.sample(3))
fintech_df_encoded.sample(3)

# %%
# save encoded
fintech_df = fintech_df_encoded

# %% [markdown]
# ## 4.22 - Findings and conlcusions

# %%


# %% [markdown]
# ## 4.3 - Normalization 

# %%
for col in fintech_df.select_dtypes(include=np.number).columns:
    print(col, ":   min=" , fintech_df[col].min(), " max=", fintech_df[col].max())

# %%
plot_columns(fintech_df, fintech_df.select_dtypes(include=np.number).columns, 0)

# %% [markdown]
# ## 4.31 - Findings and conclusions

# %% [markdown]
# We can notice that most of the columns have small values near each other, except for the `funded_amount` and `loan_amount` columns. So we can choose to normalize the data. Also, `installment_per_month` values reaches 1500s, so we can normalize it as well.
# 
# We can use the Log Transformation to normalize the data. why? because it is suitable for data that is continuous. It also helps in the skewness of the data because most of the chosen columns are skewed to the right.

# %%
# use log transformation
def normalize_columns(df: pd.DataFrame, cols: list = None) -> pd.DataFrame:
    fintech_df_normalized = df.copy()
    cols = ['loan_amount', 'funded_amount', 'installment_per_month'] if cols is None else cols
    for col in cols:
        fintech_df_normalized[col] = np.log1p(fintech_df_normalized[col])
    return fintech_df_normalized

fintech_df_normalized = normalize_columns(fintech_df)
for col in fintech_df_normalized.select_dtypes(include=np.number).columns:
    print(col, ":   min=" , fintech_df_normalized[col].min(), " max=", fintech_df_normalized[col].max())

# %%
# save normalized
fintech_df = fintech_df_normalized

# %% [markdown]
# # 5 - Lookup Table(s)

# %% [markdown]
# I generated a lookup table globally, then in each function, I updated it with the new values for each step, imputations, encodings, etc.

# %%
lookup_table.to_csv(data_dir + 'lookup_table.csv', index=False)
lookup_table.sample(10)

# %%


# %% [markdown]
# # 6 - Bonus ( Data Integration )

# %% [markdown]
# ## Web Scraping using BeautifulSoup

# %%
from bs4 import BeautifulSoup
import requests
import lxml.html as lh

# %%
scrape_url = 'https://www23.statcan.gc.ca/imdb/p3VD.pl?Function=getVD&TVD=53971'
response = requests.get(scrape_url)

soup = BeautifulSoup(response.text, 'lxml')

dom = lh.fromstring(str(soup))

# %% [markdown]
# We can get the XPath of the table and rows, then loop through the rows and get the data. 
# 
# As we can see, all rows have the same structure and nearly the same XPath.

# %% [markdown]
# The reason for this is that I am used to using the XPath to get the data, and it is more accurate than using the class name or the tag name.

# %%

# Alpha Code XPath
alpha_code_xpath =      '/html/body/main/div[3]/div/div/table/tbody/tr[*]/td[3]'
full_state_name_xpath = '/html/body/main/div[3]/div/div/table/tbody/tr[*]/td[1]'
alpha_codes = dom.xpath(alpha_code_xpath)
full_state_names = dom.xpath(full_state_name_xpath)

state_codes = [code.text for code in alpha_codes]
state_names = [name.text for name in full_state_names]

state_codes, state_names

# %% [markdown]
# Now after getting the data, we can convert it to a Dictionary for example, and use it to update the dataset.

# %%
code_state_dict = dict(zip(state_codes, state_names))

print(code_state_dict)
    

# %% [markdown]
# **BINGO!** We have successfully imported the data from the website.

# %% [markdown]
# ## Adding the new data to the dataset

# %%
fintech_df['state_name'] = fintech_df['state'].transform(lambda x: code_state_dict[x] if x in code_state_dict else 'Unknown')

fintech_df.sample(3)

# %% [markdown]
# ## 5- Exporting the dataframe to a csv file or parquet

# %%
# save the cleaned dataset
pq.write_table(pa.Table.from_pandas(fintech_df), data_dir + 'fintech_data_MET_P2_52_20136_clean.parquet')

