import logging 
import time
from typing import Text
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# Import personal module. 
from config.config_logger import setup_logger
from config.config import (
    DRIVER_PATH, WEBPAGE_LOADING_TIMEOUT, 
    LOG_SELENIUM_FILEPATH, SLEEP, 
)



# --------------------------------------------------------------
# Logging configuration 
# --------------------------------------------------------------

logger = logging.getLogger(__name__)
logger, file_handler, stream_handler = setup_logger(logger, LOG_SELENIUM_FILEPATH) 



# -------------------------------------------------------
# Loader
# -------------------------------------------------------

def launch_browser():
    # Settings for launching Chrome. 
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--incognito")
    options.add_argument("--headless")

    # Set the browser to load the URL. 
    browser = webdriver.Chrome(executable_path=DRIVER_PATH, options=options)
    return browser


def wait_for_webpage_to_load(func):
    def wrapper(url:Text, xpath:Text, **kwargs):
        # Launch the browser and load the URL. 
        browser = launch_browser()
        browser.get(url)

        # You can't scrape the data until the site completes the load. 
        # So wait for it to load first. 
        try:
            time.sleep(SLEEP)
            WebDriverWait(browser, WEBPAGE_LOADING_TIMEOUT).until(
                EC.visibility_of_element_located((By.XPATH, xpath))
            )
        # Raise an error if it takes too long. 
        except TimeoutException:
            logger.exception(f"Timed out waiting for page to load for ({func}).") 
            browser.quit()
        
        # Return the loaded page. 
        return func(url=url, xpath=xpath, browser=browser, **kwargs)
    return wrapper
