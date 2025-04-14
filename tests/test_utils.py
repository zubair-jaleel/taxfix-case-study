"""
    Description: This Python script uses pytest library to run below unit tests on 'utils' module

    Owner: Zubair Ali Jaleel, Senior Data Engineer

"""

from datetime import datetime
import sys
sys.path.append('../taxfix-case-study')

import pytest

from utils import http_request, generate_date_range


def test_http_request():
    """
    This function test the http calls to API endpoint
    """

    response = http_request('https://api.ipify.org/?format=json')

    assert 'ip' in response
    assert len(response['ip'].split('.')) == 4

def test_generate_date_range():
    """
    This function test whether date ranges are generated accurately
    """

    assert generate_date_range('2024-12-30', '2025-01-31', 20) == [
                                                                    ('2024-12-30', '2025-01-18'),
                                                                    ('2025-01-19', '2025-01-31')
                                                                    ]
    assert generate_date_range('2023-01-30', '2025-03-31') == [
                                                                ('2023-01-30', '2024-01-29'),
                                                                ('2024-01-30', '2025-01-28'),
                                                                ('2025-01-29', '2025-03-31')
                                                                ]
    assert generate_date_range('2025-01-31') == [
                                                ('2025-01-31', datetime.now().strftime('%Y-%m-%d'))
                                                ]
    assert generate_date_range('2025-03-31', '2023-01-30') == []
    # Test whether wrong input date format raises ValueError
    with pytest.raises(ValueError, match=r".*does not match format '%Y-%m-%d'"):
        generate_date_range('25-03-31')
