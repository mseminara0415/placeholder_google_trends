import calendar
import datetime
import functools
import json
from pprint import pprint
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import sys
import time

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

# def get_date_range(start_date:str, end_date:str) -> list:
#     '''_summary_
#     Get list of date ranges for the months between start and end dates.
#     For example, with a start date of 1/1/22 and an end date of 3/31/22 we would expect
#     the following output:

#     ['01-01-21 01-31-21', '02-01-21 02-28-21', '03-01-21 03-31-21']

#     Parameters
#     ----------
#     start_date : str
#         _description_
#         Should always be on the first of the month.
#     end_date : str
#         _description_
#         Should always be on the last day of the month.

#     Returns
#     -------
#     list
#         _description_
#         List of date ranges for months between start and end dates
#     '''
#     # Get month start and end dates based on parameters
#     month_start_list = pd.date_range(start=start_date, end=end_date, freq='MS').strftime('%Y-%m-%d').to_list()
#     month_end_list = pd.date_range(start=start_date, end=end_date, freq='M').strftime('%Y-%m-%d').to_list()

#     # Zip together month start and month end lists
#     if len(month_start_list) != len(month_end_list):
#         raise Exception("Month start and end lists are not the same length. Dates given for inputs must be on the first and last days of the month.")
#     else:
#         month_date_range_list = [f"{month_start} {month_end}" for month_start, month_end in zip(month_start_list, month_end_list)]
#         return month_date_range_list

# def get_google_trends(timerange:str, keyword:list) -> list:
#     '''_summary_
#     Input a timerange (i.e.'2019-03-01 2019-03-31') and a list of keyword(s)
#     and return a Pandas dataframe with Google Trend data. Please read the Google Trends documentation
#     to better understand both how to interpret the trend score and how the timerange parameter impacts this.
#     https://support.google.com/trends/answer/4365533?hl=en

#     Parameters
#     ----------
#     timerange : str
#         _description_
#         Timerange with which you want to get trend data for. Please see
#         the above Google docs link to understand how this affects the data returned.
#     keyword : list
#         _description_
#         keyword to get trend data for.

#     Returns
#     -------
#     pd.DataFrame
#         _description_
#         Return list of dictionaries for interest overtime trend data.
#     '''
#     # Build pytrends request object
#     pytrends = TrendReq(hl='en-US')

#     # Build payload for Google Trends
#     pytrends.build_payload(
#         kw_list=keyword,
#         timeframe=timerange,
#         cat=41
#     )

#     # Get trend overtime data
#     interest_over_time_trend_data = pytrends.interest_over_time()

#     return interest_over_time_trend_data

# def example_testing():
#         # Define date range we want to pull trend data for
#         date_range_list = get_date_range(start_date='3-1-19', end_date='5-31-19')

#         # Define keywords we want to get trends for
#         keyword_list = ['Call of Duty', 'Modern Warfare']

#         # Build daily trend list of dictionaries
#         # This ends up being one flat list of dictionaries for all dates requested for all keywords specified 
#         final_daily_trend_data = []

#         for target_keyword in keyword_list:
#             keyword_daily_trend_list = []
#             for target_date_range in date_range_list:

#                 # Build dataframe for a specific keywords daily data based on target date range
#                 target_keyword_trend_list = get_google_trends('all', [target_keyword])

#                 # Extend a single list for all dates for a specific keyword
#                 keyword_daily_trend_list.extend(target_keyword_trend_list)

#             # Extend final daily trend data for all specified dates and keywords
#             final_daily_trend_data.extend(keyword_daily_trend_list)

#         pprint(final_daily_trend_data)
# 
# 
class ResponseError(Exception):
    """Something was wrong with the response from Google"""

    def __init__(self, message, response):
        super(Exception, self).__init__(message)

        # pass response so it can be handled upstream
        self.response = response        

class GoogleTrendApi():
    '''
    Google Trends API.

    A majority of this code comes directly from the Pytrends library.
    Pypi Docs: https://pypi.org/project/pytrends/
    Github Repo: https://github.com/GeneralMills/pytrends
    '''
    GET_METHOD = 'get'
    POST_METHOD = 'post'
    GENERAL_URL = 'https://trends.google.com/trends/api/explore'
    INTEREST_OVER_TIME_URL = 'https://trends.google.com/trends/api/widgetdata/multiline'
    ERROR_CODES = (500, 502, 504, 429)
    
    def __init__(self, hl='en-US', tz=360, geo='', timeout=(2, 5), proxies='',
                 retries=0, backoff_factor=0, requests_args=None):
        """
        Initialize default values for params
        """
        # google rate limit
        self.google_rl = 'You have reached your quota limit. Please try again later.'
        self.results = None
        # set user defined options used globally
        self.tz = tz
        self.hl = hl
        self.geo = geo
        self.kw_list = list()
        self.timeout = timeout
        self.proxies = proxies  # add a proxy option
        self.retries = retries
        self.backoff_factor = backoff_factor
        self.proxy_index = 0
        self.requests_args = requests_args or {}
        self.cookies = self.GetGoogleCookie()
        # intialize widget payloads
        self.token_payload = dict()
        self.interest_over_time_widget = dict()

    def GetGoogleCookie(self):
        '''
        Gets google cookie (used for each and every proxy; once on init otherwise)
        Removes proxy from the list on proxy error
        '''
        while True:
            if "proxies" in self.requests_args:
                try:
                    return dict(filter(lambda i: i[0] == 'NID', requests.get(
                        'https://trends.google.com/?geo={geo}'.format(
                            geo=self.hl[-2:]),
                        timeout=self.timeout,
                        **self.requests_args
                    ).cookies.items()))
                except:
                    continue
            else:
                if len(self.proxies) > 0:
                    proxy = {'https': self.proxies[self.proxy_index]}
                else:
                    proxy = ''
                try:
                    return dict(filter(lambda i: i[0] == 'NID', requests.get(
                        'https://trends.google.com/?geo={geo}'.format(
                            geo=self.hl[-2:]),
                        timeout=self.timeout,
                        proxies=proxy,
                        **self.requests_args
                    ).cookies.items()))
                except requests.exceptions.ProxyError:
                    print('Proxy error. Changing IP')
                    if len(self.proxies) > 1:
                        self.proxies.remove(self.proxies[self.proxy_index])
                    else:
                        print('No more proxies available. Bye!')
                        raise
                    continue

    def GetNewProxy(self):
        """
        Increment proxy INDEX; zero on overflow
        """
        if self.proxy_index < (len(self.proxies) - 1):
            self.proxy_index += 1
        else:
            self.proxy_index = 0

    def _get_data(self, url, method=GET_METHOD, trim_chars=0, **kwargs):
        """Send a request to Google and return the JSON response as a Python object
        :param url: the url to which the request will be sent
        :param method: the HTTP method ('get' or 'post')
        :param trim_chars: how many characters should be trimmed off the beginning of the content of the response
            before this is passed to the JSON parser
        :param kwargs: any extra key arguments passed to the request builder (usually query parameters or data)
        :return:
        """
        s = requests.session()
        # Retries mechanism. Activated when one of statements >0 (best used for proxy)
        if self.retries > 0 or self.backoff_factor > 0:
            retry = Retry(total=self.retries, read=self.retries,
                          connect=self.retries,
                          backoff_factor=self.backoff_factor,
                          status_forcelist=GoogleTrendApi.ERROR_CODES,
                          method_whitelist=frozenset(['GET', 'POST']))
            s.mount('https://', HTTPAdapter(max_retries=retry))

        s.headers.update({'accept-language': self.hl})
        if len(self.proxies) > 0:
            self.cookies = self.GetGoogleCookie()
            s.proxies.update({'https': self.proxies[self.proxy_index]})
        if method == GoogleTrendApi.POST_METHOD:
            response = s.post(url, timeout=self.timeout,
                              cookies=self.cookies, **kwargs,
                              **self.requests_args)  # DO NOT USE retries or backoff_factor here
        else:
            response = s.get(url, timeout=self.timeout, cookies=self.cookies,
                             **kwargs, **self.requests_args)  # DO NOT USE retries or backoff_factor here
        # check if the response contains json and throw an exception otherwise
        # Google mostly sends 'application/json' in the Content-Type header,
        # but occasionally it sends 'application/javascript
        # and sometimes even 'text/javascript
        if response.status_code == 200 and 'application/json' in \
                response.headers['Content-Type'] or \
                'application/javascript' in response.headers['Content-Type'] or \
                'text/javascript' in response.headers['Content-Type']:
            # trim initial characters
            # some responses start with garbage characters, like ")]}',"
            # these have to be cleaned before being passed to the json parser
            content = response.text[trim_chars:]
            # parse json
            self.GetNewProxy()
            return json.loads(content)
        else:
            # error
            raise ResponseError(
                'The request failed: Google returned a '
                'response with code {0}.'.format(response.status_code),
                response=response)

    def build_payload(self, kw_list, cat=0, timeframe='today 5-y', geo='',
                      gprop=''):
        """Create the payload for related queries, interest over time and interest by region"""
        if gprop not in ['', 'images', 'news', 'youtube', 'froogle']:
            raise ValueError('gprop must be empty (to indicate web), images, news, youtube, or froogle')
        self.kw_list = kw_list
        self.geo = geo or self.geo
        self.token_payload = {
            'hl': self.hl,
            'tz': self.tz,
            'req': {'comparisonItem': [], 'category': cat, 'property': gprop}
        }

        # build out json for each keyword
        for kw in self.kw_list:
            keyword_payload = {'keyword': kw, 'time': timeframe,
                               'geo': self.geo}
            self.token_payload['req']['comparisonItem'].append(keyword_payload)
        # requests will mangle this if it is not a string
        self.token_payload['req'] = json.dumps(self.token_payload['req'])
        # get tokens
        self._tokens()
        return

    def _tokens(self):
        """Makes request to Google to get API tokens for interest over time, interest by region and related queries"""
        # make the request and parse the returned json
        widget_dicts = self._get_data(
            url=GoogleTrendApi.GENERAL_URL,
            method=GoogleTrendApi.GET_METHOD,
            params=self.token_payload,
            trim_chars=4,
        )['widgets']
        # order of the json matters...
        first_region_token = True

        # assign requests
        for widget in widget_dicts:
            if widget['id'] == 'TIMESERIES':
                self.interest_over_time_widget = widget

        return

    def categories(self):
        """Request available categories data from Google's API and return a dictionary"""

        params = {'hl': self.hl}

        req_json = self._get_data(
            url=GoogleTrendApi.CATEGORIES_URL,
            params=params,
            method=GoogleTrendApi.GET_METHOD,
            trim_chars=5,
            **self.requests_args
        )
        return req_json

    def interest_over_time(self):
        """Request data from Google's Interest Over Time section and return a list of dictionaries"""

        over_time_payload = {
            # convert to string as requests will mangle
            'req': json.dumps(self.interest_over_time_widget['request']),
            'token': self.interest_over_time_widget['token'],
            'tz': self.tz
        }

        # make the request and parse the returned json
        req_json = self._get_data(
            url=GoogleTrendApi.INTEREST_OVER_TIME_URL,
            method=GoogleTrendApi.GET_METHOD,
            trim_chars=5,
            params=over_time_payload,
        )
        
        trend_req_json = req_json['default']['timelineData']

        # Note the date in the response from Google is in UTC time
        # Therefore we use the method utcfromtimestamp instead of fromtimestamp.
        # This makes sure our date field is consistent with Googles response
        for response in trend_req_json:
            response['value'] = response['value'][0]
            response['date'] = datetime.datetime.utcfromtimestamp(float(response['time'])).strftime('%Y-%m-%d')
            for kw in self.kw_list:
                response['keyword'] = kw

        return trend_req_json

def get_last_date_of_month(year: int, month: int) -> datetime.date:
    '''
    Given a year and a month returns an instance of the date class
    containing the last day of the corresponding month.

    Source: https://stackoverflow.com/questions/42950/get-last-day-of-the-month-in-python
    '''
    return datetime.date(year, month, calendar.monthrange(year, month)[1])

def convert_dates_to_timeframe(start: datetime.date, stop: datetime.date) -> str:
    """
    Given two dates, returns a stringified version of the interval between
    the two dates which is used to retrieve data for a specific time frame
    from Google Trends.
    """
    return f"{start.strftime('%Y-%m-%d')} {stop.strftime('%Y-%m-%d')}"

def fetch_data(google_trends, build_payload, timeframe: str) -> list:
    """Attempts to fecth data and retries in case of a ResponseError."""
    attempts, fetched = 0, False
    while not fetched:
        try:
            build_payload(timeframe=timeframe)
        except ResponseError as err:
            print(err)
            print(f'Trying again in {60 + 5 * attempts} seconds.')
            time.sleep(60 + 5 * attempts)
            attempts += 1
            if attempts > 3:
                print('Failed after 3 attemps, abort fetching.')
                break
        else:
            fetched = True            
    return google_trends.interest_over_time()   


def get_daily_data(keyword: str, start_year: int, start_month: int, end_year: int, end_month: int, geo:str = 'US'):
    start_date = datetime.date(start_year, start_month, 1)
    stop_date = get_last_date_of_month(end_year, end_month)
    print(f"start_date: {start_date}")
    print(f"end_date: {stop_date}")
    print(f"testing: {convert_dates_to_timeframe(start_date, stop_date)}")

    google_trends = GoogleTrendApi()

    build_payload = functools.partial(google_trends.build_payload, kw_list=[keyword], cat=41, geo=geo)

    # Google will only return monthly data when looking back 5 years (260 weeks). If you give
    # google trends a time range less than that, say 3 years, it will return weekly data points

    

    # monthly_data = fetch_data(google_trends, build_payload, '2019-01-01 2019-12-01')
    monthly_data = fetch_data(google_trends, build_payload, convert_dates_to_timeframe(start=start_date, stop=stop_date))
    print(len(monthly_data))
    pprint(monthly_data)

if __name__ == "__main__":

    test = get_daily_data(
        'Call of Duty',
        start_year=2017,
        start_month=9,
        end_year=2022,
        end_month=8
    )










# ======== Prior Code using Pandas df ========
    
# def main():

#     # Define date range we want to pull trend data for
#     date_range_list = get_date_range(start_date='3-1-19', end_date='4-30-19')

#     # Define keywords we want to get trends for
#     keyword_list = ['Call of Duty', 'Call of Duty: Modern Warfare']

#     # ========= Build daily trend dataframe ========
#     # Build dataframe for specific keyword and months worth of daily data and then concatenate all dataframes
#     # to end up with daily trend data for one specific keyword
#     final_daily_trend_df_list = []

#     for target_keyword in keyword_list:
#         target_keyword_daily_df_list = []
#         for target_date_range in date_range_list:
#             # build dataframe for a specific keywords daily data based on target date range (month)
#             target_keyword_daily_trend_df = get_google_trends(target_date_range, [target_keyword])
#             target_keyword_daily_trend_df.rename(columns={f'{target_keyword}': 'daily_trend_score'}, inplace=True)
#             target_keyword_daily_trend_df['keyword'] = target_keyword
#             target_keyword_daily_df_list.append(target_keyword_daily_trend_df)
    
#         # This dataframe will only include trend data for one keyword
#         final_keyword_df = pd.concat(target_keyword_daily_df_list)
#         final_daily_trend_df_list.append(final_keyword_df)

#     # This dataframe will include trend data for all specified keywords
#     final_daily_trend_df = pd.concat(final_daily_trend_df_list)

#     # ========= Build monthly trend dataframe ========
#     final_monthly_trend_df_list = []
#     for target_keyword in keyword_list:
#         target_keyword_monthly_trend_df = get_google_trends('all', [target_keyword])
#         target_keyword_monthly_trend_df.rename(columns={f'{target_keyword}': 'daily_trend_score'}, inplace=True)
#         target_keyword_monthly_trend_df['keyword'] = target_keyword
#         final_monthly_trend_df_list.append(target_keyword_monthly_trend_df)

#     final_monthly_trend_df = pd.concat(final_monthly_trend_df_list)

#     # final_trend_df.to_csv('output3.csv') 
    
    