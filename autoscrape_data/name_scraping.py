# %%
import logging
import datetime as dt
from typing import Optional, List, Dict, Text, Tuple
from selenium.webdriver.chrome.webdriver import WebDriver

# For building data pipeline. 
from prefect import task
from prefect.engine.results import LocalResult

# For data processing and analysis. 
import numpy as np

# Import personal module. 
from autoscrape_data.selenium_loader import wait_for_webpage_to_load 
from config.config_logger import setup_logger
from config.config import (
    LOG_NAME_SCRAPING_FILEPATH, 
    URL_CARD,
    VARS_SAVE_DIR, 
)



# %%
# --------------------------------------------------------------
# Logging configuration 
# --------------------------------------------------------------

logger = logging.getLogger(__name__)
logger, file_handler, stream_handler = setup_logger(logger, LOG_NAME_SCRAPING_FILEPATH) 



# %%
# -------------------------------------------------------
# Scrape bank names and credit cards
# -------------------------------------------------------

@wait_for_webpage_to_load
@task(
    cache_for=dt.timedelta(days=1), 
    result=LocalResult(dir="result_config"), 
    checkpoint=True, 
    target="{task_name}–{date}", 
)
def compile_bank_names_for_card(url:Text, xpath:Text, browser:Optional[WebDriver]=None) -> List[Text]:
    logger.info(f"Start compiling the bank_names from ({url})!")

    # Find the element inside the HTML tags. 
    element_bank_menu = browser.find_element_by_xpath(xpath)
    element_banks = element_bank_menu.find_elements_by_tag_name('option')

    # Loop through each element except for the first index: 'All bank'. 
    ls_banks = [element.get_attribute('value') for element in element_banks[1:]]
    logger.debug(f"----- List of banks -- ({ls_banks})")
    
    browser.quit()
    np.save(f'{VARS_SAVE_DIR}/ls_banks_for_card.npy', ls_banks) 
    return ls_banks


@task(
    cache_for=dt.timedelta(days=1), 
    result=LocalResult(dir="result_config"), 
    checkpoint=True, 
    target="{task_name}–{date}", 
)
def compile_credit_cards(ls_banks:List[Text], xpath:Text) -> Dict[Text, List[Text]]:

    @wait_for_webpage_to_load
    def _compile(url:Text, xpath:Text, browser:Optional[WebDriver]=None) -> List[Text]:
        logger.info(f"Start compiling the credit cards from ({url})!")

        # Find the element inside the HTML tags. 
        element_cards_section = browser.find_element_by_xpath(xpath)
        element_cards = element_cards_section.find_elements_by_tag_name('img')

        # Loop through each element to extract the value. 
        ls_cards = [card.get_attribute('alt') for card in element_cards]

        browser.quit()
        return ls_cards

    logger.debug(f"----- List of banks -- ({ls_banks})")
    dict_data = {
        bank: _compile(''.join([URL_CARD, f'?filter={bank}']), xpath) for bank in ls_banks 
    } 
    np.save(f'{VARS_SAVE_DIR}/dict_cards.npy', dict_data) 
    return dict_data
