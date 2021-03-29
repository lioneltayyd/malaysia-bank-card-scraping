# %%
import datetime as dt

# For building data pipeline. 
from prefect import Flow
from prefect.schedules import IntervalSchedule
from prefect.engine.results import LocalResult

# For scraping (personal module). 
from config.config import (
    PIPELINE_VERSION, 
    URL_CARD, 
)
from autoscrape_data import (
    card_scraping, 
    name_scraping, 
)
from autoprocess_data import process_card_data



# %%
# -------------------------------------------------------
# Pipeline scehduler 
# -------------------------------------------------------

schedule = IntervalSchedule(interval=dt.timedelta(days=30)) 



# %%
# -------------------------------------------------------
# Build pipeline 
# -------------------------------------------------------

with Flow(name='malaysia_bank_card_scraping_flow', result=LocalResult(dir="result_config")) as flow: 

    # Step 1: Compile a list of bank names for credit cards. 
    ls_banks_for_card = name_scraping.compile_bank_names_for_card(URL_CARD, '''/html/body/main/section/form/label/select''') 

    # Step 2: Compile a list of credit cards for each bank. 
    dict_cards = name_scraping.compile_credit_cards(
        upstream_tasks=[ls_banks_for_card], 
        ls_banks=ls_banks_for_card, 
        xpath='''/html/body/main/section/ul''',
    ) 

    # Step 3: Run the scrapers. 
    df_card = card_scraping.card_scraping_procedure(
        upstream_tasks=[ls_banks_for_card, dict_cards], 
        url=URL_CARD, 
        ls_banks=ls_banks_for_card, 
        dict_data=dict_cards, 
    ) 

    # Step 4: Perform data processing and computation. 
    df_reward = process_card_data.extract_reward_points_data(upstream_tasks=[df_card], df_main=df_card) 
    df_cashback = process_card_data.extract_cashback_data(upstream_tasks=[df_card], df_main=df_card) 



# %%
# -------------------------------------------------------
# Run pipeline 
# -------------------------------------------------------

# # Register the pipeline with a project name. 
# flow.register(project_name='malaysia_bank_card_scraping') 

# Execute the pipeline. 
flow_state = flow.run()

# You need to install "GraphViz" to run this. 
flow.visualize(flow_state=flow_state)
