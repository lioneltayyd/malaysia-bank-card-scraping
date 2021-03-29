# %%
import logging
import datetime as dt

# For building data pipeline. 
from prefect import task
from prefect.engine.results import LocalResult

# For data processing and analysis. 
import json
import pandas as pd 

# Import personal module. 
from config.config_logger import setup_logger
from config.config import (
    LOG_PROCESS_CARD_DATA_FILEPATH, 
    CASHBACK_DF_FILEPATH, 
    CASHBACK_SAVE_DIR, 
    INCLUDED_QUALIFIED_APPLICANTS, 
    REWARD_POINTS_DF_FILEPATH, 
    REWARD_POINTS_SAVE_DIR, 
)
from config.config_naming import (
    DF_REWARD_CAT, 
    DF_IMG, 
    DF_BANK, 
    DF_CARD_NAME, 
    DF_CARD_TYPE, 
    DF_REWARD_POINTS, 
    DF_REQUIRED_INC, 
    DF_REQUIRED_APPLICANT, 
    DF_EACH_SPENDING, 
    DF_CASHBACK_RATE, 
    DF_CASHBACK_CAP, 
    DF_CASHBACK_BENCHMARK, 
    DF_CASHBACK_CAT, 
    DF_CASHBACK_FROM, 
    DF_CASHBACK_TILL, 
    DF_CASHBACK_WEEKENDS_COND, 
    DF_CASHBACK_MONTHLY_COND, 
    DF_CASHBACK_SINGLE_RECEIPT_COND, 
)



# %%
# --------------------------------------------------------------
# Logging configuration 
# --------------------------------------------------------------

logger = logging.getLogger(__name__)
logger, file_handler, stream_handler = setup_logger(logger, LOG_PROCESS_CARD_DATA_FILEPATH) 



# %%
# -------------------------------------------------------
# Data processing for card (reward points)
# -------------------------------------------------------

@task(
    cache_for=dt.timedelta(days=1), 
    result=LocalResult(dir="result_config"), 
    checkpoint=True, 
    target="{task_name}–{date}", 
)
def extract_reward_points_data(df_main:pd.DataFrame) -> pd.DataFrame: 
    '''
    Purpose : 
        Extract the reward points for credit cards. 

    Args    : 
        df_main : Credit card dataframe. 

    Output  :
        DataFrame with the reward points for each category. 
    '''

    def _extract_data_from_json(row):
        # If the category column is NaN, ignore this row. 
        if isinstance(row[DF_REWARD_CAT], float): 
            return row 

        # Extract the points and category from "reward_category" column. 
        dict_data = json.loads(row[DF_REWARD_CAT])  
        for points, category in dict_data.items(): 
            dict_reward_points_data[DF_IMG].append(row[DF_IMG])
            dict_reward_points_data[DF_BANK].append(row[DF_BANK])
            dict_reward_points_data[DF_CARD_NAME].append(row[DF_CARD_NAME])
            dict_reward_points_data[DF_CARD_TYPE].append(row[DF_CARD_TYPE])
            dict_reward_points_data[DF_REQUIRED_INC].append(row[DF_REQUIRED_INC])
            dict_reward_points_data[DF_REQUIRED_APPLICANT].append(row[DF_REQUIRED_APPLICANT])
            dict_reward_points_data[DF_REWARD_POINTS].append(points)
            dict_reward_points_data[DF_REWARD_CAT].append(category[0])
            logger.debug(f'----- Appended to list for reward data -- Category ({category}) -- Points ({points})') 
        return row

    # Prepare the variable. 
    dict_reward_points_data = {
        DF_IMG: [], 
        DF_BANK: [],
        DF_CARD_NAME: [],
        DF_CARD_TYPE: [],
        DF_REQUIRED_INC: [],
        DF_REQUIRED_APPLICANT: [],
        DF_REWARD_POINTS: [],
        DF_REWARD_CAT: [], 
    }

    try: 
        logger.info('Start extracting the reward points data!') 

        # Filter the dataframe by the required applicant type. 
        boo_filter_unqualified_applicant = df_main[DF_REQUIRED_APPLICANT].isin(INCLUDED_QUALIFIED_APPLICANTS)
        df_main = df_main[boo_filter_unqualified_applicant].copy() 
        logger.debug('----- Filtered the dataframe.') 

        # Extract data from the "reward_category" column. 
        _ = df_main.apply(_extract_data_from_json, axis=1) 
        df_extracted_data = pd.DataFrame(dict_reward_points_data) 
        logger.debug('----- Extracted the reward points data.') 
        
        # Extract string. 
        df_extracted_data[DF_EACH_SPENDING] = df_extracted_data[DF_REWARD_POINTS].str.extract(r'RM([\d\W]+)\s?') 
        df_extracted_data[DF_REWARD_POINTS] = df_extracted_data[DF_REWARD_POINTS].str.extract(r'([\d\W]+)\s(?=point)')
        logger.debug('----- Extracted the reward points and each spending values.') 

        # Process the data. 
        df_extracted_data[DF_EACH_SPENDING] = df_extracted_data[DF_EACH_SPENDING].str.replace('-', '1')
        df_extracted_data[DF_EACH_SPENDING] = df_extracted_data[DF_EACH_SPENDING].str.replace(',', '')
        logger.debug('----- Processed the reward points and each spending values.') 

        # Transform string to lowercase.
        df_extracted_data[DF_REWARD_POINTS] = df_extracted_data[DF_REWARD_POINTS].astype('str').str.lower() 
        df_extracted_data[DF_REWARD_CAT] = df_extracted_data[DF_REWARD_CAT].astype('str').str.lower() 

        # Transform dtype to integer. 
        df_extracted_data[DF_EACH_SPENDING] = pd.to_numeric(df_extracted_data[DF_EACH_SPENDING], downcast='integer') 
        df_extracted_data[DF_REWARD_POINTS] = pd.to_numeric(df_extracted_data[DF_REWARD_POINTS], downcast='integer') 
        logger.debug('----- Transformed the dtypes for reward points and each spending values.') 

        # Save the dataframe. 
        df_extracted_data.to_csv(REWARD_POINTS_DF_FILEPATH) 
        logger.info(f'Saved the dataframe to ({REWARD_POINTS_SAVE_DIR}) directory')
        return df_extracted_data 
    
    except Exception: 
        logger.exception('Exception occurs while processing the reward points data.') 



# %%
# -------------------------------------------------------
# Data processing for card (cashback) 
# -------------------------------------------------------

@task(
    cache_for=dt.timedelta(days=1), 
    result=LocalResult(dir="result_config"), 
    checkpoint=True, 
    target="{task_name}–{date}", 
)
def extract_cashback_data(df_main:pd.DataFrame) -> pd.DataFrame:
    '''
    Purpose : 
        Extract the cashback for credit cards. 

    Args    : 
        df_main : Credit card dataframe. 

    Output  :
        DataFrame with the cashback for each category. 
    '''

    def _extract_data_from_json(row):
        # If the category column is NaN, ignore this row. 
        if isinstance(row[DF_CASHBACK_CAT], float): 
            return row 

        # Extract the points and category from "cashback_category" column. 
        dict_data = json.loads(row[DF_CASHBACK_CAT]) 
        for category, cashback in dict_data.items():
            dict_cashback_data[DF_IMG].append(row[DF_IMG])
            dict_cashback_data[DF_BANK].append(row[DF_BANK])
            dict_cashback_data[DF_CARD_NAME].append(row[DF_CARD_NAME])
            dict_cashback_data[DF_CARD_TYPE].append(row[DF_CARD_TYPE])
            dict_cashback_data[DF_REQUIRED_INC].append(row[DF_REQUIRED_INC])
            dict_cashback_data[DF_REQUIRED_APPLICANT].append(row[DF_REQUIRED_APPLICANT])
            dict_cashback_data[DF_CASHBACK_RATE].append(cashback[0])
            dict_cashback_data[DF_CASHBACK_CAP].append(cashback[1])
            dict_cashback_data[DF_CASHBACK_BENCHMARK].append(cashback[2])
            dict_cashback_data[DF_CASHBACK_CAT].append(category)
            logger.debug(f'----- Appended to list for cashback data -- Category ({category}) -- Cashback ({cashback})') 
        return row 

    # Prepare the variable. 
    dict_cashback_data = {
        DF_IMG: [], 
        DF_BANK: [],
        DF_CARD_NAME: [],
        DF_CARD_TYPE: [],
        DF_REQUIRED_INC: [],
        DF_REQUIRED_APPLICANT: [],
        DF_CASHBACK_RATE: [],
        DF_CASHBACK_CAP: [], 
        DF_CASHBACK_BENCHMARK: [], 
        DF_CASHBACK_CAT: [], 
    }

    try: 
        logger.info('Start extracting the cashback data!') 

        # Filter the dataframe by the required applicant type. 
        boo_filter_unqualified_applicant = df_main[DF_REQUIRED_APPLICANT].isin(INCLUDED_QUALIFIED_APPLICANTS)
        df_main = df_main[boo_filter_unqualified_applicant].copy() 
        logger.debug('----- Filtered the dataframe.') 

        # CUSTOM: Filter specific cards that due to card info error. Need to resolve later.
        boo_filter_card = df_main['card_name'].isin(['standard_chartered_justone_platinum_mastercard'])
        df_main = df_main[~boo_filter_card].copy() 
        logger.debug('----- Filtered the card(s).') 

        # Extract data from the "cashback_category" column. 
        _ = df_main.apply(_extract_data_from_json, axis=1) 
        df_extracted_data = pd.DataFrame(dict_cashback_data) 
        logger.debug('----- Extracted the cashback data.') 

        # Extract data. 
        df_extracted_data[DF_CASHBACK_FROM] = df_extracted_data[DF_CASHBACK_BENCHMARK].str.extract(r'from RM([\d\W]+)')
        df_extracted_data[DF_CASHBACK_TILL] = df_extracted_data[DF_CASHBACK_BENCHMARK].str.extract(r'up to RM([\d\W]+)')
        df_extracted_data[DF_CASHBACK_WEEKENDS_COND] = df_extracted_data[DF_CASHBACK_BENCHMARK].str.contains('weekends') 
        df_extracted_data[DF_CASHBACK_MONTHLY_COND] = df_extracted_data[DF_CASHBACK_BENCHMARK].str.contains('monthly') 
        df_extracted_data[DF_CASHBACK_SINGLE_RECEIPT_COND] = df_extracted_data[DF_CASHBACK_BENCHMARK].str.contains('single receipt') 
        logger.debug('----- Extracted the cashback rate, cap, and range values') 

        # Process the data. 
        boo_no_benchmark = df_extracted_data[DF_CASHBACK_BENCHMARK].str.contains('any amount') 
        df_extracted_data.loc[boo_no_benchmark, DF_CASHBACK_FROM] = '0'
        df_extracted_data[DF_CASHBACK_FROM] = df_extracted_data[DF_CASHBACK_FROM].str.replace(',', '') 
        df_extracted_data[DF_CASHBACK_TILL] = df_extracted_data[DF_CASHBACK_TILL].str.replace(',', '') 
        df_extracted_data[DF_CASHBACK_RATE] = df_extracted_data[DF_CASHBACK_RATE].str.replace('%', '') 
        df_extracted_data[DF_CASHBACK_CAP] = df_extracted_data[DF_CASHBACK_CAP].str.replace('RM', '') 
        df_extracted_data[DF_CASHBACK_CAP] = df_extracted_data[DF_CASHBACK_CAP].str.replace(',', '') 
        df_extracted_data[DF_CASHBACK_CAP] = df_extracted_data[DF_CASHBACK_CAP].str.replace('uncapped', '1e+10') 
        logger.debug('----- Processed the cashback rate, cap, and range values') 

        # Transform string to lowercase. 
        df_extracted_data[DF_CASHBACK_CAT] = df_extracted_data[DF_CASHBACK_CAT].astype('str').str.lower() 

        # Transform dtype to integer. 
        df_extracted_data[DF_CASHBACK_FROM] = pd.to_numeric(df_extracted_data[DF_CASHBACK_FROM], downcast='integer') 
        df_extracted_data[DF_CASHBACK_TILL] = pd.to_numeric(df_extracted_data[DF_CASHBACK_TILL], downcast='integer') 
        df_extracted_data[DF_CASHBACK_RATE] = pd.to_numeric(df_extracted_data[DF_CASHBACK_RATE], downcast='integer') 
        df_extracted_data[DF_CASHBACK_CAP] = pd.to_numeric(df_extracted_data[DF_CASHBACK_CAP], downcast='integer') 
        logger.debug('----- Transformed the dtypes for cashback rate, cap, and spending range.') 

        # Fill in the null value. 
        df_extracted_data.loc[df_extracted_data[DF_CASHBACK_FROM].isnull(), DF_CASHBACK_FROM] = 0 

        # Fix the error. Swap the values if the value of 'cashback_from' is larger than 'cashback_till'. 
        boo_range_error = df_extracted_data[DF_CASHBACK_FROM] > df_extracted_data[DF_CASHBACK_TILL] 
        df_extracted_data['cashback_from_copy'] = df_extracted_data[DF_CASHBACK_FROM] 
        df_extracted_data.loc[boo_range_error, DF_CASHBACK_FROM] = df_extracted_data.loc[boo_range_error, DF_CASHBACK_TILL] 
        df_extracted_data.loc[boo_range_error, DF_CASHBACK_TILL] = df_extracted_data.loc[boo_range_error, 'cashback_from_copy']
        df_extracted_data.drop(columns=['cashback_from_copy'], inplace=True) 
        logger.debug('----- Fixed the error for cashback spending range.') 

        # Save the dataframe. 
        df_extracted_data.to_csv(CASHBACK_DF_FILEPATH, index=False) 
        logger.info(f'Saved the dataframe to ({CASHBACK_SAVE_DIR}) directory') 
        return df_extracted_data

    except Exception: 
        logger.exception('Exception occurs while processing the cashback data.') 
