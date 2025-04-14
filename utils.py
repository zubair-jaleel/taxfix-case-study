"""
    Description: This Python script contains utility classes and functions that will be used by the main ETL 
    extraction script to accomplish tasks that are not core to the ETL extraction logic, such as making 
    http calls with retries and pagination logic, common logic across all ETLs, creating connection to 
    external data sources/sink, getting/saving data into data sources/sink, adding metadata, etc.

    Owner: Zubair Ali Jaleel, Senior Data Engineer

"""

import json
import logging
from time import sleep
import configparser
from typing import Any, Dict, List, Union, Tuple
from datetime import date, datetime, timedelta

import requests

config = configparser.ConfigParser()
config.read('config.ini')
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(filename)s: %(message)s')


def http_request(url: str, request_type: str = 'GET', headers: Dict = None, request_params: Dict = None,
                 request_json: Dict = None, request_data: Union[Dict, str] = None, retries: int = 0) -> Any:
    """
    This function does a http request with given url and request parameters. If API call returns error,
    then it'll retry few times with some interval

    :param url: API url
    :param request_type: http request type (GET or POST)
    :param headers: http request header (dict)
    :param request_params: http request parameters
    :param request_json: http request json body
    :param request_data: http request data
    :param retries: nth retry for the same url

    :return: JSON/dict response data

    """

    try:
        response = requests.request(request_type, url=url, headers=headers, params=request_params,
                                    json=request_json, data=request_data, timeout=30)
        response_json = response.json()

    except (requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError, json.decoder.JSONDecodeError) as e:
        if retries <= 2:
            logging.warning('Waiting and retrying (retry_count=%s) after Error: %s.', retries + 1, str(e))
            sleep(60)
            return http_request(url, request_type, headers, request_params, request_json, request_data, retries=retries + 1)

        raise

    # Throttling due to API Rate limit (I'm assuming a rate limit of 2 requests per second)
    sleep(float(config['FAKER']['rate_limit_sleep_time']))

    if response.status_code == 200:
        return response_json

    if retries <= 2:
        logging.warning('HTTP Response %s Error: %s.\nWaiting and retrying (retry_count=%s)',
                     response.status_code, response.text, retries + 1)
        sleep(60)
        return http_request(url, request_type, headers, request_params, request_json, request_data, retries=retries + 1)

    raise Exception(f"Error in Faker API's Persons endpoint, response code %s. URL: {url}. "
                    f"HTTP status_code: {response.status_code}. Details: {response.text}")

def generate_date_range(start_date: Union[str, datetime], end_date: Union[str, datetime] = datetime.now(),
                        date_interval: Union[str, int] = int(config['FAKER']['date_interval'])) -> List[Tuple]:
    """
    This function gets list of date range for the given start date, end date & interval

    :param start_date: start date of the date range
    :param end_date: end date of the date range, default would be yesterday
    :param date_interval: number of days in difference within a tuple

    :return: list of date ranges
    
    """

    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    if isinstance(date_interval, str):
        date_interval = int(date_interval)

    dates_generated = [start_date + timedelta(days=x)
                       for x in range(0, (end_date - start_date).days + 1, date_interval)
                       ]
    date_range = [(datetime.strftime(dt, '%Y-%m-%d'),
                   datetime.strftime(min(end_date, dt + timedelta(days=date_interval-1)), '%Y-%m-%d'))
                  for dt in dates_generated if dt != datetime.combine(date.today(), datetime.min.time())
                  ]

    return date_range
