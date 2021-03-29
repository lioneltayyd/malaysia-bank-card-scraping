# __Malaysia Credit Cards Scraper__



## __Project Purpose__

To scrape cards data and analyse the credit cards market. 



## __Main Tools__

Tools | Description
:--- | :---
[selenium][selenium_docs_url] | For web scraping. 
[prefect][prefect_docs_url] | For building data pipeline and dashboard. 
[pandas][pandas_docs_url] | For data processing. 
[pipenv][pipenv_docs_url] | For managing dependencies. 



## __Data Architecture Overview__

This project focuses on the __Prefect Pipeline__ section. It's associated with [Bank Card Dashboard Repo][malaysia_bank_card_dashboard_repo]. 

![Project Architecture Diagram][architecture_overview_img] 

The following diagram shows the flow of the pipeline. 

![Prefect Pipeline Flow Diagram][pipeline_flow_img] 



## __Folder Structure__

File / Folder Name | Description
:--- | :---
autoprocess_data | For keeping custom python modules related to data processing. 
autoscrape_data | For keeping custom python modules related to scraping. 
config | For configuration. It encompasses 3 files. `config_logger` is for logger, `config_naming` is for namings, `config` is for other general configuration. 
result_config | For caching output when running `Prefect` pipeline. 
docs | For storing files, data, and documents. 
logs | For storing the log info. 
sh | For running bash script on Mac. 
pipfile | For setting up the virtual environment and tracking all the installed dependencies. 



## __Project Setup__

1.  Pull the project from the repo. 

1.  Install `pyenv` to manage the python version (if needed) and `pipenv` for dependencies. 

1.  Run this to install dependencies from the `Pipfile.lock` file. 
    
    ```bash
    pipenv shell;
    pipenv sync; 
    ```

1.  To reinstall the entire dependencies, update the version within the `Pipfile` file if needed, then run this. This will 
    automatically create / update the `Pipfile.lock` file for you. 

    ```bash
    pipenv install;
    ```

1.  Try import specific library and check the version to see if it's installed. 



## __Code Running Guide__

1.  Configure the scraping rate limit inside the `config/config.py` file. 

    ![Rate Limit Configuration Example][config_rate_limit_img]

1.  Run the scraper manually in 2 ways. 

    Bash script.

      ```bash
      sh sh/run_pipeline.sh
      ```

    Command line.

      ```bash
      python3 run_pipeline.py
      ```



## __Deployment Guide__

No deployment setup for this project yet. 



## __Debugging & Testing__ 

1.  Assign `True` to the `DEBUG` variable. 

    ![Debug Configuration Example][config_debug_img]

1.  To debug specific functions or code, use [Jupytext Percent Format][jupytext_percent_docs_url]. 
    Simple include the following at the bottom of the `run_pipeline.py` file and write your code 
    to test specific function(s). This relies on `ipykernel` module so ensure that it is installed 
    via `pipenv`. 

    ![Jupytext Percent Example][jupytext_percent_img]



[architecture_overview_img]: ./docs/images/architecture_overview.jpg 
[pipeline_flow_img]: ./docs/images/pipeline_dag.png
[config_rate_limit_img]: ./docs/images/config_rate_limit_example.png
[config_debug_img]: ./docs/images/config_debug_example.png
[jupytext_percent_img]: ./docs/images/jupytext_percent_example.png

[malaysia_bank_card_dashboard_repo]: https://github.com/lioneltayyd/malaysia_bank_card_dashboard

[prefect_docs_url]: https://docs.prefect.io/
[selenium_docs_url]: https://selenium-python.readthedocs.io/
[pandas_docs_url]: https://pandas.pydata.org/docs/user_guide/index.html
[pipenv_docs_url]: https://pipenv-fork.readthedocs.io/en/latest/
[jupytext_percent_docs_url]: https://jupytext.readthedocs.io/en/latest/formats.html#the-percent-format
