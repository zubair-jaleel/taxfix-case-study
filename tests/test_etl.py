"""
    Description: This Python script uses pytest library to run below unit & integration tests on 'etl' module

    Owner: Zubair Ali Jaleel, Senior Data Engineer

"""

import json
import sys
sys.path.append('../taxfix-case-study')

import pytest

from etl import get_age_range, anonymize_pii, etl_pipeline


def test_get_age_range():
    """
    This function test whether get_age_range() returns correct age range for a given date of birth
    """

    assert get_age_range('2024-10-11') == '[01-10]'
    assert get_age_range('2000-10-11') == '[21-30]'
    assert get_age_range('1900-11-21') == '[121-130]'
    # Test whether wrong input date format raises ValueError
    with pytest.raises(ValueError, match=r".*does not match format '%Y-%m-%d'"):
        get_age_range('25-03-31')
    # Test whether wrong input date of birth (future date) raises ValueError
    with pytest.raises(ValueError, match='Given date of birth is in future'):
        get_age_range('2027-03-31')

def test_anonymize_pii():
    """
    This function test whether anonymize_pii() anonymizes all the PII as given in the case study
    """

    with open('tests/data/person.json', 'r', encoding='utf-8') as file:

        person = json.loads(file.read())
        anonymized_person = anonymize_pii(person)

        assert anonymized_person['id'] == 121
        assert anonymized_person['firstname'] == '****'
        assert anonymized_person['lastname'] == '****'
        assert anonymized_person['email_domain'] == 'luettgen.com'
        assert anonymized_person['phone'] == '****'
        assert anonymized_person['age_range'] == '[121-130]'
        assert anonymized_person['gender'] == 'male'
        assert anonymized_person['address']['id'] == 1
        assert anonymized_person['address']['street'] == '****'
        assert anonymized_person['address']['zipcode'] == '****'
        assert anonymized_person['address']['coordinates']['latitude'] == '****'


def test_etl_pipeline():
    """
    This function test the completion of etl pipeline end-to-end
    """

    try:
        etl_pipeline('2023-10-15')
    except Exception as e:
        assert False, f'ETL pipeline raised error: {e}'
