import requests
import pandas as pd
import numpy as np
import re
import json
from datetime import datetime
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt

def url_request(url: str):
    """
    This function will do a request to the given url and return a response object
    """
    try:
        request = requests.get(url)
    except requests.exceptions.ConnectionError as error:
        print(f'Connection error ocurred. Check internet connection. Error: {error}')
    except requests.exceptions.Timeout as error:
        print(f'The request reached timeout. Error: {error}')
    except requests.exceptions.HTTPError as error:
        print(f'An HTTP error ocurred. Error: {error}')
    except requests.exceptions.RequestException as error:
        print(f'An error ocurred. Error: {error}')
    return request

def web_scraping(request):
    """
    This function receives a response object and parses the text on it to gather specific data
    Returns the berry url template and a list of tupples with attribute names and descriptions
    """
    soup = BeautifulSoup(request.text, features = "html.parser")
    list_url_templates = []
    paragraphs = soup.find_all('p')
    for paragraph in paragraphs:
        if "GET https://" in paragraph.text:
            pattern = r" .*"
            url_template = re.search(pattern, paragraph.text)[0].strip()
            list_url_templates.append(url_template)
            if "/berry/{" in url_template:
                pattern = r"\{[\w ]+\}\/"
                berry_url_template = re.sub(pattern, r"", url_template)
    tables = soup.find_all('tbody')
    rows = tables[2].find_all('tr')
    list_name_desc = [(row.find_all('td')[0].text.strip(), row.find_all('td')[1].text.strip()) for row in rows]
    return (berry_url_template, list_name_desc)

def extracting_json(url, headers):
    """
    This function receives an url template that serves as base to construct the complete url
    for each existing berry on the pokeAPI. Then gets all the json data as a string and casts
    it to pyhton dict. Then creates each berry data dict is stored in a list to later populate
    a dataframe which is returned.
    """
    berry_id = 1
    json_request = url_request(f'{url}{berry_id}')
    code = json_request.status_code
    berry_dict = {}
    list_berries = []
    while code == 200:
        json_request = url_request(f'{url}{berry_id}')
        berry_data = json.loads(json_request.text)
        for header in headers:
            key = header[0]
            if key in ["firmness", "item", "natural_gift_type"]:
                berry_dict[key] = berry_data[key]["name"]
            elif key == "flavors":
                berry_dict[key] = str(berry_data[key])
            else:
                berry_dict[key] = berry_data[key]
        list_berries.append(berry_dict.copy())
        berry_id += 1
        json_request = url_request(f'{url}{berry_id}')
        code = json_request.status_code
    berry_df = pd.DataFrame.from_dict(list_berries)
    return berry_df

def transforming_data(dataframe):
    """
    This function receives a dataframe in order to calculate the required statistics.
    Then a new dataframe is generated and returned for the new calculations
    """

    headers = ["berries_names",
               "growth_times",
               "min_growth_time",
               "median_growth_time",
               "max_growth_time",
               "variance_growth_time",
               "mean_growth_time",
               "frequency_growth_time"]
    
    growth_time_list = dataframe["growth_time"].to_list()
    berry_names = dataframe["name"].to_list()
    min_growth_time = np.min(growth_time_list)
    median_growth_time = np.median(growth_time_list)
    max_growth_time = np.max(growth_time_list)
    variance_growth_time = np.var(growth_time_list)
    mean_growth_time = np.mean(growth_time_list)

    frequency_growth_time = {}
    for growth_time in growth_time_list:
        frequency_growth_time[growth_time] = growth_time_list.count(growth_time)

    calculations = [berry_names,
                    growth_time_list,
                    min_growth_time,
                    median_growth_time, max_growth_time,
                    variance_growth_time, mean_growth_time,
                    frequency_growth_time]
    
    records = dict(zip(headers, calculations))
    transformed_df = pd.DataFrame.from_dict([records])
    return transformed_df

def load_to_csv(dataframe, csv_file):
    """
    This function receives a dataframe and generates a CSV file from it for reporting purposes.
    """
    dataframe.to_csv(csv_file, index = False)
    return dataframe

def log_progress(message: str, log_file: str):
    """
    This function generates a txt faile that contains records with a timestamp for each stage of the process
    """
    print(message)
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    current_time = datetime.now() # get current timestamp 
    timestamp = current_time.strftime(timestamp_format)
    with open(log_file, "a") as logging_file:
        logging_file.write(timestamp + ',' + message + '\n')

poke_url = "https://pokeapi.co/docs/v2#berries-section"
stats_output_file = "poke-API_statistics.csv"
raw_output_file = "all_berries_data.csv"
log_file = "log_file.txt"

log_progress("Requesting data to pokeAPI", log_file)
api_request = url_request(poke_url)

log_progress("Performing some web scraping to gather data...", log_file)
berry_url_template = web_scraping(api_request)[0]
headers = web_scraping(api_request)[1]

log_progress("Extracting juice from berries...", log_file)
berry_df = extracting_json(berry_url_template, headers)

log_progress("Transforming berries data", log_file)
transfromed_berry_df = transforming_data(berry_df)

log_progress("Generating CSV file", log_file)
load_to_csv(transfromed_berry_df, stats_output_file)
print(load_to_csv(berry_df, raw_output_file))

log_progress("Ploting Histogram", log_file)
plt.figure(figsize=(8, 6))
plt.hist(transfromed_berry_df['frequency_growth_time'].values[0].keys(), weights=transfromed_berry_df['frequency_growth_time'].values[0].values(), bins=len(transfromed_berry_df['frequency_growth_time'].values[0]))
plt.title('Frequency of Growth Time')
plt.xlabel('Growth Time')
plt.ylabel('Frequency')
plt.grid(True)
plt.show()