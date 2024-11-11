from sqlalchemy import create_engine
import pandas as pd

SCHEMA_WITHOUT_ADDS = ['customer_id', 'emp_title', 'emp_length', 'home_ownership',
       'annual_inc', 'annual_inc_joint', 'verification_status', 'zip_code',
       'addr_state', 'avg_cur_bal', 'tot_cur_bal', 'loan_status',
       'loan_amount', 'state', 'funded_amount', 'term', 'int_rate', 'grade',
       'issue_date', 'pymnt_plan', 'type', 'purpose', 'description']
FULL_SCHEMA = ['customer_id','emp_title','emp_length','annual_inc','annual_inc_joint','zip_code','addr_state','avg_cur_bal','tot_cur_bal','loan_amount','state','funded_amount','term','int_rate','grade','issue_date','pymnt_plan','description','month_number','salary_can_cover','letter_grade','installment_per_month','addr_state_encoded','state_encoded','letter_grade_encoded','home_ownership_other','home_ownership_own','home_ownership_rent','verification_status_source_verified','verification_status_verified','purpose_credit_card','purpose_debt_consolidation','purpose_home_improvement','purpose_house','purpose_major_purchase','purpose_medical','purpose_moving','purpose_other','purpose_renewable_energy','purpose_small_business','purpose_vacation','purpose_wedding','type_individual','type_joint','loan_status_current','loan_status_default','loan_status_fully_paid','loan_status_in_grace_period','loan_status_late_(16_30_days)','loan_status_late_(31_120_days)']


engine = create_engine('postgresql://root:root@pgdatabase:5432/fintechdb')

def save_to_db(cleaned : pd.DataFrame, append : bool, tablename : str='fintech_data_MET_P02_52_20136_clean'):
    if(engine.connect()):
        print('Connected to Database')
        try:
            print('Writing cleaned dataset to database')
            cleaned.to_sql(tablename, con=engine, if_exists=('fail' if not append else 'append'))
            print('Done writing to database')
        except ValueError as vx:
            print('Cleaned Table already exists.')
        except Exception as ex:
            print(ex)
    else:
        print('Failed to connect to Database')

methods = {
    'emp_title': 'mode',
    'int_rate': 'mean',
    'annual_inc_joint': 'annual_inc',
    'emp_length': 'mode'
}


def get_imputation_from_db(row, tablename : str='fintech_data_MET_P02_52_20136_clean', column : str='annual_inc'):
    if(engine.connect()):
        print('Connected to Database')
        try:
            print('Reading imputed data from database')
            if methods[column] == 'annual_inc':
                val = row['annual_inc']
            elif methods[column] == 'mean':
                val = pd.read_sql_query(f"SELECT CAST(AVG({column})) AS double precision FROM public.\"{tablename}\"", con=engine)
            else:
                val = pd.read_sql_query(f"SELECT MODE() WITHIN GROUP (ORDER BY {column}) FROM public.\"{tablename}\"", con=engine)


            print('Done reading from database')
            return val
        except ValueError as vx:
            print('Table does not exist.')
        except Exception as ex:
            print(ex)
    else:
        print('Failed to connect to Database')
        return None

                             

