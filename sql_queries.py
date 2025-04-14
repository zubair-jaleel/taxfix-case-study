"""
    Description: This file contains sql queries to run against DuckDB table; Used by the other scripts.

    Owner: Zubair Ali Jaleel, Senior Data Engineer

"""

GERMAN_GMAIL_USERS_PERCENTAGE_SQL = """
    WITH german_gmail_users AS 
    (
        SELECT 
            COUNT(*) AS cnt 
        FROM 
            df_anonymized_persons 
        WHERE 
            LOWER(address_country) = 'germany' and STARTS_WITH(LOWER(email_domain), 'gmail.')
    )
    SELECT cnt * 100 / (SELECT COUNT(*) FROM df_anonymized_persons) AS german_gmail_users_percentage
    FROM german_gmail_users
"""

TOP_3_GMAIL_COUNTRIES_SQL = """
    WITH gmail_users_count AS 
    (
        SELECT 
            LOWER(address_country) AS country,
            COUNT(*) AS cnt
        FROM 
            df_anonymized_persons 
        WHERE 
            STARTS_WITH(LOWER(email_domain), 'gmail.')
        GROUP BY 1
    )
    SELECT 
        country, RANK () OVER (ORDER BY cnt DESC) AS rank
    FROM 
        gmail_users_count
    QUALIFY 
        rank <=3
"""

TOP_3_GMAIL_COUNTRIES_SQL_2 = """
    SELECT 
        LOWER(address_country) AS country,
        COUNT(*) AS gmail_users_count
    FROM 
        df_anonymized_persons 
    WHERE 
        STARTS_WITH(LOWER(email_domain), 'gmail.')
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 3
"""

GMAIL_USERS_OVER_60_SQL = """
    SELECT 
        COUNT(*) AS count_people_over_60
    FROM 
        df_anonymized_persons 
    WHERE 
        REPLACE(REGEXP_REPLACE(age_range, '-.*', ''), '[', '')::INTEGER > 60
"""
