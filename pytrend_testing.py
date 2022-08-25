import calendar
from cgi import test
import datetime
from typing import final
import pandas as pd
from pytrends.request import TrendReq
from pprint import pprint

# ========= Resources ==========
# Google Trends Doc - https://support.google.com/trends/answer/4365533?hl=en
# pytrends Doc - https://pypi.org/project/pytrends/
# Example of getting daily data and aggregating - https://medium.com/@bewerunge.franz/google-trends-how-to-acquire-daily-data-for-broad-time-frames-b6c6dfe200e6
# Partial Data - http://searchanalysisguide.blogspot.com/2013/04/google-trends-what-is-partial-data.html


#  ========= Questions We'll want to understand ==========
# What is the goal of having this data? (intrest over time analysis)
# What grain will we want this data at? (ideally daily)
# What if any filtering will we want to do? (Geographic holistic and then country level, 'Video Game Series' Category for franchise (Call of Duty) and 'Video Game' Category for titles)
# What should the start date be? (March 1st 2019)

# What keywords? Breakouts by Premium Title Since MW19 (MW19, BOCW, VG, MW2), Warzone, Warzone 2.0 + state breakouts
# franchise or title

# ========= End Table Sketch =========
# Date    Keyword    Daily_Score    Monthly_Score    Adjusted_Score (Daily_Score x 0.Monthly_Score)

#  ========= Script ==========


def get_date_range(start_date:str, end_date:str) -> list:
    '''_summary_
    Get list of date ranges for the months between start and end dates.
    For example, with a start date of 1/1/22 and an end date of 3/31/22 we would expect
    the following output:

    ['01-01-21 01-31-21', '02-01-21 02-28-21', '03-01-21 03-31-21']

    Parameters
    ----------
    start_date : str
        _description_
        Should always be on the first of the month.
    end_date : str
        _description_
        Should always be on the last day of the month.

    Returns
    -------
    list
        _description_
        List of date ranges for months between start and end dates
    '''
    # Get month start and end dates based on parameters
    month_start_list = pd.date_range(start=start_date, end=end_date, freq='MS').strftime('%Y-%m-%d').to_list()
    month_end_list = pd.date_range(start=start_date, end=end_date, freq='M').strftime('%Y-%m-%d').to_list()

    # Zip together month start and month end lists
    if len(month_start_list) != len(month_end_list):
        raise Exception("Month start and end lists are not the same length. Dates given for inputs must be on the first and last days of the month.")
    else:
        month_date_range_list = [f"{month_start} {month_end}" for month_start, month_end in zip(month_start_list, month_end_list)]
        return month_date_range_list

def get_google_trends(timerange:str, keyword:list) -> pd.DataFrame:
    '''_summary_
    Input a timerange (i.e.'2019-03-01 2019-03-31') and a list of keyword(s)
    and return a Pandas dataframe with Google Trend data. Please read the Google Trends documentation
    to better understand both how to interpret the trend score and how the timerange parameter impacts this.
    https://support.google.com/trends/answer/4365533?hl=en

    Parameters
    ----------
    timerange : str
        _description_
        Timerange with which you want to get trend data for. Please see
        the above Google docs link to understand how this affects the data returned.
    keyword : list
        _description_
        keyword to get trend data for.

    Returns
    -------
    pd.DataFrame
        _description_
        Return Pandas Dataframe for interest overtime trend data.
    '''
    # Build pytrends request object
    pytrends = TrendReq(hl='en-US')

    # Build payload for Google Trends
    pytrends.build_payload(
        kw_list=keyword,
        timeframe=timerange,
        cat=41
    )

    # Get dataframe of intrest overtime data
    interest_over_time_trend_data = pytrends.interest_over_time()

    return interest_over_time_trend_data


def main():

    # Define date range we want to pull trend data for
    date_range_list = get_date_range(start_date='3-1-19', end_date='4-30-19')

    # Define keywords we want to get trends for
    keyword_list = ['Call of Duty', 'Call of Duty: Modern Warfare']

    # ========= Build daily trend dataframe ========
    # Build dataframe for specific keyword and months worth of daily data and then concatenate all dataframes
    # to end up with daily trend data for one specific keyword
    final_daily_trend_df_list = []

    for target_keyword in keyword_list:
        target_keyword_daily_df_list = []
        for target_date_range in date_range_list:
            # build dataframe for a specific keywords daily data based on target date range (month)
            target_keyword_daily_trend_df = get_google_trends(target_date_range, [target_keyword])
            target_keyword_daily_trend_df.rename(columns={f'{target_keyword}': 'daily_trend_score'}, inplace=True)
            target_keyword_daily_trend_df['keyword'] = target_keyword
            target_keyword_daily_df_list.append(target_keyword_daily_trend_df)
    
        # This dataframe will only include trend data for one keyword
        final_keyword_df = pd.concat(target_keyword_daily_df_list)
        final_daily_trend_df_list.append(final_keyword_df)

    # This dataframe will include trend data for all specified keywords
    final_daily_trend_df = pd.concat(final_daily_trend_df_list)

    # ========= Build monthly trend dataframe ========
    final_monthly_trend_df_list = []
    for target_keyword in keyword_list:
        target_keyword_monthly_trend_df = get_google_trends('all', [target_keyword])
        target_keyword_monthly_trend_df.rename(columns={f'{target_keyword}': 'daily_trend_score'}, inplace=True)
        target_keyword_monthly_trend_df['keyword'] = target_keyword
        final_monthly_trend_df_list.append(target_keyword_monthly_trend_df)

    final_monthly_trend_df = pd.concat(final_monthly_trend_df_list)

    # final_trend_df.to_csv('output3.csv') 

def example_testing():
        # Define date range we want to pull trend data for
        date_range_list = get_date_range(start_date='3-1-19', end_date='5-31-19')

        # Define keywords we want to get trends for
        keyword_list = ['Call of Duty', 'Modern Warfare']

        # Build daily trend list of dictionaries
        # This ends up being one flat list of dictionaries for all dates requested for all keywords specified 
        final_daily_trend_data = []

        for target_keyword in keyword_list:
            keyword_daily_trend_list = []
            for target_date_range in date_range_list:

                # Build dataframe for a specific keywords daily data based on target date range
                target_keyword_trend_list = get_google_trends('all', [target_keyword])

                # Extend a single list for all dates for a specific keyword
                keyword_daily_trend_list.extend(target_keyword_trend_list)

            # E 
            final_daily_trend_data.extend(keyword_daily_trend_list)

        pprint(final_daily_trend_data)
        
                
                


        

if __name__ == "__main__":
    # main()
    # testing()
    example_testing()
    
    