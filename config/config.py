import logging



# -------------------------------------------------------
# General config 
# -------------------------------------------------------

DRIVER_PATH = "docs/system/chromedriver_v88" 
SLEEP = 5
WEBPAGE_LOADING_TIMEOUT = 10 + SLEEP

# List of values. 
CARD_TYPE = set(['mastercard', 'visa', 'american', 'unionpay']) 

# List of data to include and exclude when scraping. 
INCLUDED_BANKS = []
INCLUDED_QUALIFIED_APPLICANTS = ['Anybody', "Malaysians Only", "Malaysians and Permanent Residents"] 
EXCLUDED_SUMMARY = ['Balance Transfer', 'Easy Payment Plan', 'Interest-free', 'Cashback'] 
INCLUDED_SUMMARY = ['Min. Income', 'Annual Fee', 'Interest Rate'] 
INCLUDED_REQUIREMENTS = ['Minimum Age', 'Who Can Apply'] 

# Regex. 
RX_QUALIFIED_APPLICANT = r"Anybody|Malaysians|Permanent Residents|Salaried employee|Self-employed"

# URL. 
URL_CARD = "https://ringgitplus.com/en/credit-card/"

# Versioning. 
PIPELINE_VERSION = 1
DF_CARD_VERSION = 1 
DF_CASHBACK_VERSION = 1
DF_REWARD_POINTS_VERSION = 1 

# Directory path for saving files. 
VARS_SAVE_DIR = "docs/variables"
CARD_SAVE_DIR = "docs/csv/card"
CASHBACK_SAVE_DIR = "docs/csv/cashback" 
REWARD_POINTS_SAVE_DIR = "docs/csv/reward_points"

# Directory path for saving checkpoints. 
CARD_CHECKPOINT_DIR = "docs/csv/card_scraping_checkpoint" 

# Path to the CSV files. 
CARD_DF_FILEPATH = f"{CARD_SAVE_DIR}/df_card_v{DF_CARD_VERSION}.csv" 
CASHBACK_DF_FILEPATH = f"{CASHBACK_SAVE_DIR}/df_cashback_v{DF_CASHBACK_VERSION}.csv" 
REWARD_POINTS_DF_FILEPATH = f"{REWARD_POINTS_SAVE_DIR}/df_reward_points_v{DF_REWARD_POINTS_VERSION}.csv" 

# Path to the log files. 
LOG_SELENIUM_FILEPATH = "logs/selenium_loader.log"
LOG_CARD_SCRAPING_FILEPATH = "logs/card_scraping.log"
LOG_NAME_SCRAPING_FILEPATH = "logs/name_scraping.log"
LOG_PROCESS_CARD_DATA_FILEPATH = "logs/process_card_data.log" 



# -------------------------------------------------------
# Config for variable names 
# -------------------------------------------------------

# General metadata. 
DF_URL = 'url'
DF_IMG = 'img'
DF_BANK = 'bank'

# Card metadata. 
DF_CARD_NAME_ORIGINAL = 'card_name_original'
DF_CARD_NAME = 'card_name'
DF_CARD_TYPE = 'card_type'

# Card feature. 
DF_CARD_FEATURE = 'card_feature_description'
DF_CARD_BENEFIT = 'card_benefit_description'

# Card cost. 
DF_COST_CARD_INT_RATE = 'cost_interest_rate_annum'
DF_COST_FEE = 'cost_annual_fee'
DF_COST_FEE_COND = 'cost_annual_fee_condition'

# Requirement.
DF_REQUIRED_INC = 'required_income'
DF_REQUIRED_AGE = 'required_age'
DF_REQUIRED_APPLICANT = 'required_applicant_type'

# Cashback info. 
DF_CASHBACK = 'cashback'
DF_CASHBACK_CAT = 'cashback_category'
DF_CASHBACK_INFO = 'cashback_info'
DF_CASHBACK_RATE = 'cashback_rate'
DF_CASHBACK_CAP = 'cashback_cap' 
DF_CASHBACK_BENCHMARK = 'cashback_benchmark' 
DF_CASHBACK_FROM = 'cashback_from'
DF_CASHBACK_TILL = 'cashback_till'
DF_CASHBACK_WEEKENDS_COND = 'cashback_weekends_only'
DF_CASHBACK_MONTHLY_COND = 'cashback_monthly_basis'
DF_CASHBACK_SINGLE_RECEIPT_COND = 'cashback_single_receipt'

# Reward info. 
DF_REWARD = 'reward'
DF_REWARD_CAT = 'reward_category'
DF_REWARD_INFO = 'reward_info'
DF_REWARD_POINTS = 'reward_points'
DF_EACH_SPENDING = 'each_spending'

# Travel benefit. 
DF_TRAVEL_BENEFIT = 'travel_benefit'
DF_TRAVEL_BENEFIT_CAT = 'travel_benefit_category'
DF_TRAVEL_BENEFIT_INFO = 'travel_benefit_info'

# Premium info. 
DF_PREMIUM = 'premium'
DF_PREMIUM_INFO = 'premium_info'

# Petrol info. 
DF_PETROL = 'petrol'
DF_PETROL_INFO = 'petrol_info'

# Requirement. 
DF_REQUIRED_INC_RANGE = 'required_income_range'
DF_REQUIRED_MIN_INC = 'required_min_income'
DF_REQUIRED_MAX_INC = 'required_max_income'
DF_REQUIRED_APPLICANT = 'required_applicant'
DF_REQUIRED_AGE = 'required_age'
DF_REQUIRED_MIN_AGE = 'required_min_age'
DF_REQUIRED_MAX_AGE = 'required_max_age'

# Penalty cost. 
DF_COST_PENALTY = 'cost_late_fee_penalty'



# -------------------------------------------------------
# Dataframe config for card scraping 
# -------------------------------------------------------

CARD_DATA = {
    DF_URL: [], 
    DF_IMG: [], 
    DF_BANK: [], 
    DF_CARD_NAME: [], 
    DF_CARD_NAME_ORIGINAL: [], 
    DF_CARD_TYPE: [], 
    DF_CARD_FEATURE: [], 
    DF_CARD_BENEFIT: [], 
    DF_COST_CARD_INT_RATE: [], 
    DF_COST_FEE: [], 
    DF_COST_FEE_COND: [], 
    DF_REQUIRED_INC: [], 
    DF_REQUIRED_AGE: [], 
    DF_REQUIRED_APPLICANT: [], 
    DF_CASHBACK: [], 
    DF_CASHBACK_CAT: [], 
    DF_CASHBACK_INFO: [], 
    DF_REWARD: [], 
    DF_REWARD_CAT: [], 
    DF_REWARD_INFO: [], 
    DF_TRAVEL_BENEFIT: [], 
    DF_TRAVEL_BENEFIT_CAT: [], 
    DF_TRAVEL_BENEFIT_INFO: [], 
    DF_PREMIUM: [], 
    DF_PREMIUM_INFO: [], 
    DF_PETROL: [], 
    DF_PETROL_INFO: [], 
}
