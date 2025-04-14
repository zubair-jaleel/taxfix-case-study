"""
    Title: Taxfix Case Study for Senior Data Engineer

    Description: This Python script extract data from Faker API's persons endpoint and stores it in polars 
    DataFrame. DuckDB (Embedded columnar-vectorized sql query engine) is used to do analysis on it and print
    the results to standard output console

    Owner: Zubair Ali Jaleel, Senior Data Engineer

"""

import re
import logging
import configparser
from typing import Dict, List
from datetime import date, datetime, UTC

import polars as pl
import duckdb

import utils
import sql_queries


config = configparser.ConfigParser()
config.read('config.ini')
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(filename)s: %(message)s')

def get_age_range(dob: str) -> str:
    """
    This function will calculate decadal age range for given date of birth

    :param dob: date of birth in string format YYYY-MM-DD

    :return: decadal age range

    """

    today = date.today()
    dob = datetime.strptime(dob, "%Y-%m-%d").date()
    age_in_years = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    if age_in_years < 0:
        raise ValueError('Given date of birth is in future')

    # Ages from 11 until 20 will fall in age_range [11-20]
    # Ages from 21 until 30 will fall in age_range [21-30], so on..
    # Children less than 1 year old are put into age_range [01-10]
    if age_in_years == 0:
        age_range = '[01-10]'
    elif age_in_years % 10 == 0:
        age_range = f'[{age_in_years//10 - 1}1-{age_in_years//10}0]'
    else:
        age_range = f'[{age_in_years//10}1-{age_in_years//10 + 1}0]'

    return age_range

def anonymize_pii(person: Dict) -> Dict:
    """
    This function anonymizes the person json object which we got from faker person api.
    It applies anonymization rules one-by-one to all json keys

    :param person: unanonymized input dictionary

    :return: anonymized dictionary

    """

    pii_fields = [
        'firstname', 'lastname', 'phone', 'street', 'streetName',
        'buildingNumber','zipcode', 'latitude', 'longitude'
        ]

    anonymized_person = {}
    for key, value in person.items():
        if isinstance(value, dict):
            # Anonymize recursively
            anonymized_person[key] = anonymize_pii(person[key])

        elif key in pii_fields:
            # Replace PII fields with '****'
            anonymized_person[key] = '****'

        elif key == 'email':
            # Extract domain from email address
            anonymized_person['email_domain'] = re.sub('.*@', '', person['email'])

        elif key == 'birthday':
            # Replace date of birth with decadal age range
            anonymized_person['age_range'] = get_age_range(person['birthday'])

        else:
            anonymized_person[key] = person[key]

    return anonymized_person

def extract_persons_from_api(start_date: str = None) -> List[Dict]:
    """
    This function calls the Faker API's persons endpoint, returns the data as list of dictionary.
    
    Extraction Logic: We need to extract data of around 30k person, however one API call would return maximum of
    1000 entries. There's no pagination logic implemented in the API, therefore we need to iterate over birthdate
    while fetching data from API. The number of entries per API call (quantity) is decided by config
    parameters (import_data_size & date_interval) and that's constant for all API calls, therefore number of
    persons will be evenly distributed across each API call. For simplicity I made date_interval as 1 year (365 days)

    :param start_date: date to start the extract from, Be default its the start_date from config file

    :return: List of persons dictionary

    """

    logging.info('Extracting data from Faker Persons API...')

    persons = []
    date_range = utils.generate_date_range(start_date if start_date else config['FAKER']['start_date'])
    quantity = str(int(config['FAKER']['import_data_size']) // (len(date_range) - 1 if len(date_range) > 1 else 1))

    for start_date, end_date in date_range:
        persons_api_url = f'{config['FAKER']['base_url']}/persons'
        persons_api_url += f'?_quantity={quantity}&_birthday_start={start_date}&_birthday_end={end_date}'
        response = utils.http_request(persons_api_url)

        if response['status'] == 'OK' and response['code'] == 200:
            persons += response['data']

    return persons

def anonymize_data(persons: List[Dict], timestamp_utc_str: str) -> pl.DataFrame:
    """
    This function anonymize all Personally Identifiable Information (PII) based on rules given in the case study
    and converts it into polars dataframe, so that duckdb can analyze efficiently

    :param persons: list of dictionary contains person's data
    :param timestamp_utc_str: timestamp of when we extracted the data from API

    :return: polars dataframe containing anonymized data

    """

    logging.info('Anonymizing PII data...')

    anonymized_persons = []

    for person in persons:
        anonymized_person = anonymize_pii(person)
        anonymized_person['extracted_ts_utc'] = timestamp_utc_str
        anonymized_persons.append(anonymized_person)

    df_anonymized_persons = pl.json_normalize(anonymized_persons, separator='_')

    return df_anonymized_persons

def analyze_data(df_anonymized_persons: pl.DataFrame) -> None:
    """
    This function uses duckdb to analyze data in polars dataframe (in-memory) using standard sql and prints out
    the result to standard output

    :param df_anonymized_persons: polars dataframe containing anonymized data

    """

    logging.info('Total number of rows extracted from Faker API: %s', len(df_anonymized_persons))

    german_gmail_users = duckdb.sql(sql_queries.GERMAN_GMAIL_USERS_PERCENTAGE_SQL).fetchall()[0][0]
    logging.info('Percentage of users live in Germany and use Gmail = %s', format(german_gmail_users, '.4f'))

    result = duckdb.sql(sql_queries.TOP_3_GMAIL_COUNTRIES_SQL).fetchall()
    top_3_gmail_users_countries = [str(x[1]) + '. ' + x[0] for x in result]
    logging.info('Top three countries with their rank (window function) that use Gmail: %s', top_3_gmail_users_countries)

    result = duckdb.sql(sql_queries.TOP_3_GMAIL_COUNTRIES_SQL_2).fetchall()
    top_3_gmail_users_countries = [x[0] for x in result]
    logging.info('Top three countries (as per groupby method) that use Gmail: %s', top_3_gmail_users_countries)

    count_people_over_60 = duckdb.sql(sql_queries.GMAIL_USERS_OVER_60_SQL).fetchall()[0][0]
    logging.info('No. of people over 60 years use Gmail = %s', count_people_over_60)

def etl_pipeline(start_date: str = None) -> None:
    """
    This function calls above functions to extract, anonymize and analyze Faker API's persons data
    
    :param start_date: date to start the extract from, Be default its the start_date from config file

    """

    try:
        # extract timestamp is inserted into data which might be useful for analysis downstream
        timestamp_utc_str = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        persons = extract_persons_from_api(start_date)
        df_anonymized_persons = anonymize_data(persons, timestamp_utc_str)
        analyze_data(df_anonymized_persons)

    except Exception as e:
        logging.error('Unable to run the ETL pipeline because of Error: %s', str(e))
        raise

if __name__ == '__main__':

    # Run the ETL pipeline
    etl_pipeline()
