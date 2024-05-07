import pytest
from code_challenge_api import url_request, web_scraping, extracting_json, transforming_data

def test_successful_response():
    testcase = "https://pokeapi.co/docs/v2#berries-section"
    assert url_request(testcase).status_code == 200 
    
def test_failed_response():
    testcase = "https://pokeapi.co/api/v2/item/0"
    assert url_request(testcase).status_code != 200, "Server not Found"

def test_url_berry_template():
    request = url_request("https://pokeapi.co/docs/v2#berries-section")
    testcase = "https://pokeapi.co/api/v2/berry/"

    assert web_scraping(request)[0] == testcase

def test_header_names():
    request = url_request("https://pokeapi.co/docs/v2#berries-section")
    testcase = ["id", "name", "growth_time", "max_harvest", "natural_gift_power", "size", "smoothness", "soil_dryness", "firmness", "flavors", "item", "natural_gift_type"]
    print(web_scraping(request)[1][0])
    for i in range (0, len(testcase)):
        assert web_scraping(request)[1][i][0] == testcase[i]

def test_header_lenght():
    request = url_request("https://pokeapi.co/docs/v2#berries-section")
    testcase = ["id", "name", "growth_time", "max_harvest", "natural_gift_power", "size", "smoothness", "soil_dryness", "firmness", "flavors", "item", "natural_gift_type"]
    assert len(web_scraping(request)[1]) == len(testcase)

def test_json_data_was_extracted():
    request = url_request("https://pokeapi.co/docs/v2#berries-section")
    url = web_scraping(request)[0]
    headers = web_scraping(request)[1]
    df = extracting_json(url, headers)  
    testcase = 64 
    assert len(df) >= testcase

def test_transformed_data_header_names():
    request = url_request("https://pokeapi.co/docs/v2#berries-section")
    url = web_scraping(request)[0]
    headers = web_scraping(request)[1]
    df = extracting_json(url, headers)
    transformed_df = transforming_data(df)
    headers = transformed_df.keys()
    testcase = ["berries_names", "growth_times", "min_growth_time", "median_growth_time", "max_growth_time", "variance_growth_time", "mean_growth_time", "frequency_growth_time"]
    for i in range(0, len(testcase)):
        assert headers[i] == testcase[i]

def test_statistics_in_dataframe():
    request = url_request("https://pokeapi.co/docs/v2#berries-section")
    url = web_scraping(request)[0]
    headers = web_scraping(request)[1]
    df = extracting_json(url, headers)
    transformed_df = transforming_data(df)
    testcase = 1
    assert len(transformed_df) == testcase
    
if __name__ == "__main__":
    pytest.main()
