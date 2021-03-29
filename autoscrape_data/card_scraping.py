# %%
import logging 
import datetime as dt
from typing import Optional, List, Dict, Text, Tuple
from selenium.webdriver.chrome.webdriver import WebDriver

# For building data pipeline. 
from prefect import task
from prefect.engine.results import LocalResult

# For data processing and analysis. 
import json, re
import numpy as np
import pandas as pd 

# Import personal module. 
from autoscrape_data.selenium_loader import wait_for_webpage_to_load 
from config.config_logger import setup_logger
from config.config import (
    LOG_CARD_SCRAPING_FILEPATH, 
    CARD_DATA, CARD_TYPE, 
    CARD_CHECKPOINT_DIR, 
    CARD_DF_FILEPATH, 
    DF_CARD_VERSION, 
)
from config.config_naming import (
    DF_IMG, 
    DF_REQUIRED_INC, 
    DF_COST_FEE, 
    DF_COST_FEE_COND, 
    DF_COST_CARD_INT_RATE, 
    DF_REQUIRED_AGE, 
    DF_REQUIRED_APPLICANT, 
    DF_REWARD, 
    DF_CASHBACK, 
    DF_TRAVEL_BENEFIT, 
    DF_PREMIUM, 
    DF_PETROL, 
    DF_CARD_NAME_ORIGINAL, 
    DF_BANK, 
    DF_CARD_NAME, 
    DF_URL, 
    DF_CARD_TYPE, 
)



# %%
# --------------------------------------------------------------
# Logging configuration 
# --------------------------------------------------------------

logger = logging.getLogger(__name__)
logger, file_handler, stream_handler = setup_logger(logger, LOG_CARD_SCRAPING_FILEPATH) 



# %%
# -------------------------------------------------------
# Helper function
# -------------------------------------------------------

def _extract_table_data_for_card(
        df_row:pd.DataFrame, col:Text, id_tag:Text, browser:WebDriver,
    ) -> pd.DataFrame:
    '''
    Purpose :
        Helper function for extracting data from a table within HTML. 

    Args    : 
        df_row  : Dataframe to append the values to. 
        col     : Name of the dataframe column to assign the data to. 
        id_tag  : HTML id tag. 
        browser : A Selenium object to open the browser. 

    Output  : 
        Updated dataframe after appending the values. 
    '''

    logger.info(f'Start scraping ({id_tag})!')

    # Set a default value. 
    df_row[col] = 'False'

    try:
        # Find the text element inside the HTML tags.  
        element_main = browser.find_element_by_id(id_tag)

        # Assign value to the designated column name. 
        df_row[col] = 'True'
        logger.debug(f'----- Added ({col})!')  

        # Assign value to the designated column named with info. 
        df_row[f'{col}_info'] = element_main.find_element_by_tag_name('p').text
        logger.debug(f'----- Added ({col}) info!') 

        # Find the table element inside the HTML tags. 
        element_rows = element_main.find_element_by_tag_name('tbody').find_elements_by_tag_name('tr')

        # Assign value to the designated column named with category. 
        dict_data = {
            element_row.find_elements_by_tag_name('td')[0].text: [ 
                data.text for data in element_row.find_elements_by_tag_name('td')[1:] 
            ] for element_row in element_rows 
        }
        df_row[f'{col}_category'] = json.dumps(dict_data)
        logger.debug(f'----- Added ({col}) categories!') 
            
    except Exception: 
        logger.warn(f'Unable to extract full data for ({col}) due to exception or data not exist.') 

    return df_row


def _extract_list_data_for_card(
        df_row:pd.DataFrame, ls_headers:List, ls_cols:List, xpath:Text, browser:WebDriver
    ) -> pd.DataFrame: 
    '''
    Purpose :
        Helper function for extracting data from a list within HTML. 

    Args    : 
        df_row      : Dataframe to append the values to. 
        ls_headers  : A list of header names to extract the data from. 
        ls_cols     : A list of dataframe column names to assign the data to. 
        xpath       : Path to the HTML tags. 
        browser     : A Selenium object to open the browser. 

    Output  : 
        Updated dataframe after appending the values. 
    '''

    logger.info('Start scraping (Requirements)!')

    try: 
        # Find the element inside the HTML tags. 
        element_main = browser.find_element_by_xpath(xpath) 
        element_headers = element_main.find_elements_by_tag_name('dt')
        element_cells = element_main.find_elements_by_tag_name('dd')
        
        # Assign value to the designated column name.
        for header, element_data in zip(element_headers, element_cells): 
            for compared_header, col in zip(ls_headers, ls_cols): 
                logger.debug(f'----- Check the header name -- ({header.text}) vs ({compared_header})!') 
                if header.text == compared_header: 
                    ls_data = [data.text for data in element_data.find_elements_by_tag_name('li')] 
                    df_row[f'{col}'] = ' | '.join(ls_data) 
                    logger.debug(f'----- Added ({header.text}) with the value ({ls_data})!') 

    except Exception: 
        logger.warn(f'Unable to extract full data for ({col}) due to exception or data not exist.') 

    return df_row


def _extract_img_for_card(df_row:pd.DataFrame, xpath:Text, browser:WebDriver): 
    '''
    Purpose :
        Helper function for extracting data from a list within HTML. 

    Args    : 
        df_row      : Dataframe to append the values to. 
        xpath       : Path to the HTML tags. 
        browser     : A Selenium object to open the browser. 

    Output  : 
        Updated dataframe after appending the values. 
    '''

    # Extract card image and assign value to the designated column. 
    logger.info('Start scraping (Image)!')
    element_img = browser.find_element_by_xpath(xpath)
    df_row[DF_IMG] = element_img.get_attribute('src')
    df_row[DF_IMG] = df_row[DF_IMG].str.replace(r'®|¬Æ', '') 
    logger.debug(f'''----- Added an image ({element_img.get_attribute('alt')})!''')

    return df_row


def _extract_summary_data_for_card(
        df_row:pd.DataFrame, 
        xpath:Text, 
        browser:WebDriver
    ) -> pd.DataFrame: 
    '''
    Purpose :
        Helper function for extracting data from a list within HTML. 

    Args    : 
        df_row      : Dataframe to append the values to. 
        xpath       : Path to the HTML tags. 
        browser     : A Selenium object to open the browser. 

    Output  : 
        Updated dataframe after appending the values. 
    '''

    logger.debug('Start scraping (Summary)!')
    element_summary = browser.find_element_by_xpath(xpath) 
    element_summary_headers = element_summary.find_elements_by_tag_name('dt') 
    element_summary_data = element_summary.find_elements_by_tag_name('dd') 

    for header, data in zip(element_summary_headers, element_summary_data):

        if re.match(r'(?i)Min\. Income\*?', header.text):
            data = data.find_element_by_tag_name('span')
            processed_value = float( str(data.text).replace('RM', '').replace(',', '') )
            df_row[DF_REQUIRED_INC] = processed_value
            logger.debug(f'----- Added ({header.text}) with the value ({processed_value})!')

        elif re.match(r'(?i)Annual Fee\*?', header.text):
            df_row[DF_COST_FEE] = data.text
            df_row[DF_COST_FEE_COND] = 'not_free'
            if re.match(r'(?i)Free\*?', data.text): 
                df_row[DF_COST_FEE] = '0'
                df_row[DF_COST_FEE_COND] = data.text.lower()
            logger.debug(f'----- Added ({header.text}) with the value ({data.text})!')

        elif re.match(r'(?i)Interest Rate\*?', header.text):
            df_row[DF_COST_CARD_INT_RATE] = data.text
            logger.debug(f'----- Added ({header.text}) with the value ({data.text})!')

    return df_row


def _compile_initial_data_for_card(
        df_row:pd.DataFrame, 
        url:Text, 
        bank:Text, 
        card:Text
    ) -> Tuple[pd.DataFrame, Text]: 

    # Keep a record of the original (unprocessed) card name in the dataframe. 
    df_row[DF_CARD_NAME_ORIGINAL] = [card] 

    # Convert url str value to slug format to get the url to the card webpage. 
    card_slug = card.replace("'", '').replace(' &', '').replace(' -', '').replace(':', '').replace('®', '').replace('¬Æ', '')
    card_slug = card_slug.replace(' ', '-') 
    card_url = ''.join([url, f'{card_slug}.html'])
    processed_card_str = card.lower().replace(' ', '_').replace('_-', '').replace('-', '_') 

    # Assign the data to the designated column. 
    df_row[DF_BANK] = [bank.lower().replace(' ', '_')]
    df_row[DF_CARD_NAME] = [processed_card_str] 
    df_row[DF_URL] = [card_url] 
    logger.debug(f'----- Added ({processed_card_str}) and ({card_url}) for ({bank})!') 

    # Extract and assign the card type to the designated column. 
    ls_splitted_card_str = processed_card_str.split('_') 
    for splitted_card_str in ls_splitted_card_str:
        if splitted_card_str in CARD_TYPE: 
            df_row[DF_CARD_TYPE] = splitted_card_str 
            break
        else: 
            df_row[DF_CARD_TYPE] = np.nan 

    return df_row, card_url


def _save_data_for_card(
        df_main:pd.DataFrame, 
        dict_data:Dict[Text, List[Text]], 
        idx_bank:int, 
        idx_item:int, 
        ls_banks:List[Text], 
        bank:Text, 
    ) -> Tuple[pd.DataFrame, bool]: 

    '''
    Purpose :
        Start scraping card data. 

    Args    : 
        df_main     : The main
        dict_data : A dict obj containing the bank names and card names.  
                    Example: 
                        {
                            bank_name: [
                                card_name_1, 
                                card_name_2, 
                                card_name_3
                            ]
                        }
        idx_bank    : The current list index of the bank name. 
        idx_item    : The current list index of the card. 
        ls_banks    : A list of banks to scrape the relevant data from.
        bank        : Bank name. 

    Output  : 
        Updated dataframe (partially completed or completed) and a boolean
        to indicate whether the scraping has completed or not. 
    '''

    # Save the scraping checkpoint. 
    scrape_completed = False
    df_main.to_csv(f'{CARD_CHECKPOINT_DIR}/df_card_checkpoint_{idx_bank}_{idx_item}.csv', index=False) 
    logger.info(f'Saved the checkpoint ({idx_bank}) -- ({idx_item})!') 

    # Save the complete dataframe if the scraping procedure has completed. 
    if (idx_bank == len(ls_banks) - 1) and (idx_item == len(dict_data[bank]) - 1):
        scrape_completed = True
        df_main.to_csv(CARD_DF_FILEPATH, index=False)  
        logger.info(f'Saved the complete dataframe as version ({DF_CARD_VERSION})!') 

    return df_main, scrape_completed



# %%
# -------------------------------------------------------
# Card scraper 
# -------------------------------------------------------

@wait_for_webpage_to_load
def _scrape_card_data(
        url:Text, xpath:Text, browser:Optional[WebDriver]=None, **kwargs
    ) -> pd.DataFrame: 
    '''
    Purpose :
        Start scraping card data. 

    Args    : 
        url     : URL to scrape the data from. 
        xpath   : Path to the HTML tags. 
        browser : A Selenium object to open the browser. 

    Output  : 
        Updated dataframe after appending the scraped data to a single row. 

    Notice  :
        You must assign a dataframe as an additional argument to this function 
        via "**kwargs" as a workaround. The reason it is not listed as part of the 
        required arguments is because the wrapper function will throw an error if an 
        additional argument is provided. Even if I make it a required argument for 
        the wrapper function, the other functions that involve the wrapper will also 
        throw an error. 
    '''

    df_row = kwargs['df_row'] 

    try:
        # Extract card image and assign value to the designated column. 
        df_row = _extract_img_for_card(df_row, xpath='''/html/body/main/header/img''', browser=browser)

        # Extract the data from the summary section and assign value to the designated column. 
        df_row = _extract_summary_data_for_card(df_row, xpath='''/html/body/main/section[1]''', browser=browser) 

        # Extract requirement and assign value to the designated column. 
        df_row = _extract_list_data_for_card(
            df_row, 
            ls_headers=['Mininum Age', 'Who Can Apply'], 
            ls_cols=[DF_REQUIRED_AGE, DF_REQUIRED_APPLICANT], 
            xpath='''//*[@id="requirements"]''', 
            browser=browser, 
        )

        # Extract reward info and assign value to the designated column. 
        df_row = _extract_table_data_for_card(df_row, col=DF_REWARD, id_tag='''rewards''', browser=browser)

        # Extract cashback info and assign value to the designated column. 
        df_row = _extract_table_data_for_card(df_row, col=DF_CASHBACK, id_tag='''cashback''', browser=browser)

        # Extract travel benefit info and assign value to the designated column. 
        df_row = _extract_table_data_for_card(df_row, col=DF_TRAVEL_BENEFIT, id_tag='''travel''', browser=browser)

        # Extract premium info and assign value to the designated column. 
        df_row = _extract_table_data_for_card(df_row, col=DF_PREMIUM, id_tag='''premium''', browser=browser)

        # Extract petrol info and assign value to the designated column. 
        df_row = _extract_table_data_for_card(df_row, col=DF_PETROL, id_tag='''petrol''', browser=browser)

    except Exception:
        logger.exception('Unable to scrape specific data due to exception.') 
    
    browser.quit()
    return df_row


@task(
    cache_for=dt.timedelta(days=1), 
    result=LocalResult(dir="result_config"), 
    checkpoint=True, 
    target="{task_name}–{date}", 
)
def card_scraping_procedure(
        url:Text, ls_banks:List[Text], dict_data:Dict[Text, List], 
    ) -> pd.DataFrame: 

    '''
    Purpose :
        Run the scraping procedure.  

    Args    : 
        url       : URL to scrape the data from. 
        ls_banks  : A list of banks to scrape the relevant data from.  
        dict_data : A dict obj containing the bank names and card names.  
                    Example: 
                        {
                            bank_name: [
                                card_name_1, 
                                card_name_2, 
                                card_name_3
                            ]
                        }
    
    Output  : 
        The complete dataframe containing all the scraped data for each bank. 
    '''

    df_main = pd.DataFrame(CARD_DATA)
    save_df = True

    # Compile the relevant data for each credit card for each bank. 
    # And only scrape from banks that are confirmed by the client. 
    for idx_bank, bank in enumerate(ls_banks):
        for idx_card, card in enumerate(dict_data[bank]):
            # NOTICE: 
            #   You can choose to include code to send an email / a notification 
            #   of the error message if the scraping fails. 
            try:
                logger.info(f'Start collecting data for ({bank}) -- ({card})!') 

                # Compile the initial data first before scraping the data. 
                df_row = pd.DataFrame(CARD_DATA)
                df_row, card_url = _compile_initial_data_for_card(df_row, url, bank, card)

                logger.info(f'Start scraping ({idx_card}) -- ({card})!')

                # REFINE: Refine the function for (_scrape_card_data) regarding the (**kwargs). 
                # For further detail, read the 'Notice' section documented under that function. 
                df_row = _scrape_card_data(url=card_url, xpath='''/html/body/main/section[1]''', df_row=df_row)
                
                # Append a new row to the main dataframe. 
                df_main = pd.concat([df_main, df_row], ignore_index=True).copy() 
                logger.debug(f'----- Added a new row to (df_main)!') 

            except Exception:
                # Don't save the dataframe if there's an error / exception. 
                save_df = False
                logger.exception('Unable to complete the scraping procedure due to exception.') 
                break 

        if save_df: 
            df_main, scrape_completed = _save_data_for_card(df_main, dict_data, idx_bank, idx_card, ls_banks, bank)
            if scrape_completed: 
                return df_main 
        else:
            logger.error('Skipped file save due to exception. Please resolve the error to proceed.') 
            break 
