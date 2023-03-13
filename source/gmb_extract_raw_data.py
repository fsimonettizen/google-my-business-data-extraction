import boto3
import os, httplib2, json, argparse
import requests
import time
import sys
import datedelta
import datetime
import re
import psycopg2
import psycopg2.extras
from string import punctuation

from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client import tools

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variable
global_headers = None
global_parameters = None

def get_text(text, language):
    ''' 
        From a determined string obtain the text from a specific language
    '''

    try:
        text = text.replace('\n','')
        if "(Original)" in text:
            if language == "pt_br":
                # pt_br
                result = text.split("(Original)",1)[1]
            elif language == "en":
                # en
                result = text.split("(Original)",1)[0].replace('(Translated by Google)','')

        elif "(Translated by Google)" in text:
            if language == "pt_br":
                # pt_br
                result = text.split("(Translated by Google)",1)[0]
            elif language == "en":
                # en
                result = text.split("(Translated by Google)",1)[1]
        elif text is '':
            result = text
        else:
            # Not implemented
            result = ""

    except Exception as e:
        print(e)
        result = text

    return result

def insert_data(conn, insert_statement):
    ''' 
        Insert data from table_name and data (dict with keys and values)
    '''
    with conn.cursor() as cur:
        try:
            cur.execute(insert_statement)
        except Exception as e:
            print(str(e))
            pass
        else:
            cur.execute('COMMIT;')
        return 

def format_and_insert_data(conn, table_name, data, entity = None):
    ''' 
        @TODO: improve this section
        Format data to create insert statement
    '''

    keys = ''
    values = ''

    def convert_field_to_sql_value(value):
        return_value = ''

        if isinstance(value, int) or isinstance(value, float) or isinstance(value, bool):
            return_value = str(value) + ", "
        elif isinstance(value, str):
            return_value = "'" + value + "', "
        else:
            return_value = "NULL, "
        return return_value

    # @TODO: Improve this instructions (maybe unify some equivalent parts)
    if table_name == "gmb_location_metrics":
        for location_name_key in data.keys():
            for start_time_key in data[location_name_key].keys():
                # Clean keys and values
                keys = ''
                values = ''

                # Set Primary Keys and values 
                if 'name' not in keys:
                    keys += '"name", '
                    values += "'" + location_name_key + "', "
                
                if 'start_time' not in keys:
                    keys += 'start_time, '
                    values += "'" + start_time_key + "', "

                for metric_key in data[location_name_key][start_time_key].keys():
                    # Set metric keys and values

                    keys += metric_key + ", "
                    value = data[location_name_key][start_time_key][metric_key]

                    values += convert_field_to_sql_value(value)

                keys = keys[:-2]
                values = values[:-2]

                insert_statement = 'INSERT INTO public."'+table_name+'" ('+keys+') VALUES('+values+');'
                insert_data(conn, insert_statement)

        # Update Parameter
        manage_parameter("update", "gmb_insights_extraction_last_date")

    elif table_name == "gmb_location":
        
        for key, value in data.items():
            keys += key + ', '
            values += convert_field_to_sql_value(value)

        keys = keys[:-2]
        values = values[:-2]        
        # prepare the insert_statement
        insert_statement = 'INSERT INTO public."'+table_name+'" ('+keys+') VALUES('+values+');'
        print(insert_statement)
        insert_data(conn, insert_statement)

    elif table_name == "words_count":
        # [0] "name", [1] review_name, [2] comment, [3] comment_pt_br, [4] create_time, [5] user_type
        if entity == "reviews": 
            keys = '"name", origin_entity, word, start_time'
        else:
            keys = '"name", origin_entity, word, start_time, user_type'

        for line in data:
            print(str(line))
            for word in line[len(line) - 1]:
                if word != 'Empty' and word != 'Not implemented':
                    if entity == "reviews": 
                        values = "'" + line["name"] + "','"+entity+"'" + ", '" + word + "', '" + line["create_time"].strftime("%Y-%m-%dT%H:%M:%S.%fZ") +"'"
                    else:
                        values = "'" + line["name"] + "','"+entity+"'" + ", '" + word + "', '" + line["create_time"].strftime("%Y-%m-%dT%H:%M:%S.%fZ") +"', '" + line["user_type"] + "' "
                                                                                                                                                 
                    insert_statement = 'INSERT INTO public."'+table_name+'" ('+keys+') VALUES('+values+');'
                    insert_data(conn, insert_statement)

    elif table_name == "aws_compreend_sentiment":
        insert_keys = ''
        values = ''

        dict_keys = data.keys()
        for dict_key in dict_keys:
            insert_keys += dict_key + ', '
            
            values += convert_field_to_sql_value(data[dict_key])

        insert_keys = insert_keys[:-2]
        values = values[:-2]

        insert_statement = 'INSERT INTO public."'+table_name+'" ('+insert_keys+') VALUES('+values+');'
        print("insert_statement " + insert_statement)
        insert_data(conn, insert_statement)
    
    elif table_name == "aws_compreend_key_phrase" or table_name == "aws_compreend_entities":
        insert_keys = ''
        values = ''

        for data_dict in data:

            dict_keys = data_dict.keys()
            for dict_key in dict_keys:
                
                insert_keys += dict_key + ', '
                values += convert_field_to_sql_value(data[dict_key])

            insert_keys = insert_keys[:-2]
            values = values[:-2]

            insert_statement = 'INSERT INTO public."'+table_name+'" ('+insert_keys+') VALUES('+values+');'
            print("insert_statement " + insert_statement)
            insert_data(conn, insert_statement)

    else:
        for data_dict in data:
            keys = ''
            values = ''
            
            for key in data_dict.keys():
                value = data_dict[key]
                keys += key + ', '

                values += convert_field_to_sql_value(value)

            keys = keys[:-2]
            values = values[:-2]

            # prepare the insert_statement
            insert_statement = 'INSERT INTO public."'+table_name+'" ('+keys+') VALUES('+values+');'
            print(insert_statement)
            insert_data(conn, insert_statement)
        
        if table_name == "gmb_questions":
            # Update Parameter
            manage_parameter("update", "gmb_questions_extraction_last_date")        
        elif table_name == "gmb_answers":
            # Update Parameter
            manage_parameter("update", "gmb_answers_extraction_last_date")

        return True
    
    return True

def get_start_and_end_time(how_many_months_from_now):
    '''
        get_start_and_end_time for query dates 
    '''
    return_object = []

    def get_start_and_end_time_from_month(month):
        '''
            internal function
        '''
        now = datetime.datetime.now()
        start_time = (now.replace(day=1, hour=0,minute=0,second=0, microsecond=0) - datedelta.datedelta(months=month))
        end_time = (now - datedelta.datedelta(months=month))

        if month == 0:
            if datetime.datetime.now().day == 1:
                end_time = now.replace(day=datetime.datetime.now().day)
            else:
                end_time = now.replace(day=datetime.datetime.now().day-1)
        else:
            end_time = start_time.replace(day=datedelta._days_in_month(start_time.year,start_time.month), hour=23, minute=59, second=59)
        return {"start_time": start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
               "end_time": end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}

    for month in range(0, how_many_months_from_now + 1): 
        # For each month in how_many_months_from_now
        date_object = get_start_and_end_time_from_month(month)
        return_object.append(date_object)
    
    return return_object

def get_location_metrics_data(account_name, location_name_address):
    '''
       Get report data insights for make graphs 
    '''
    data = {}
    data[location_name_address] = {}
    global global_headers
    global global_parameters

    metric = 'ALL'
    # METRIC_UNSPECIFIED  No metric specified.
    # ALL Shorthand to request all available metrics. Which metrics are included in ALL varies, and depends on the resource for which insights are being requested.
    # QUERIES_DIRECT  The number of times the resource was shown when searching for the location directly.
    # QUERIES_INDIRECT    The number of times the resource was shown as a result of a categorical search (for example, restaurant).
    # QUERIES_CHAIN   The number of times a resource was shown as a result of a search for the chain it belongs to, or a brand it sells. For example, Starbucks, Adidas. This is a subset of QUERIES_INDIRECT.
    # VIEWS_MAPS  The number of times the resource was viewed on Google Maps.
    # VIEWS_SEARCH    The number of times the resource was viewed on Google Search.
    # ACTIONS_WEBSITE The number of times the website was clicked.
    # ACTIONS_PHONE   The number of times the phone number was clicked.
    # ACTIONS_DRIVING_DIRECTIONS  The number of times driving directions were requested.
    # PHOTOS_VIEWS_MERCHANT   The number of views on media items uploaded by the merchant.
    # PHOTOS_VIEWS_CUSTOMERS  The number of views on media items uploaded by customers.
    # PHOTOS_COUNT_MERCHANT   The total number of media items that are currently live that have been uploaded by the merchant.
    # PHOTOS_COUNT_CUSTOMERS  The total number of media items that are currently live that have been uploaded by customers.
    # LOCAL_POST_VIEWS_SEARCH The number of times the local post was viewed on Google Search.
    # LOCAL_POST_ACTIONS_CALL_TO_ACTION   The number of times the call to action button was clicked on Google.

    options = "AGGREGATED_DAILY"
    # METRIC_OPTION_UNSPECIFIED   No metric option specified. Will default to AGGREGATED_TOTAL in a request.
    # AGGREGATED_TOTAL    Return values aggregated over the entire time frame. This is the default value.
    # AGGREGATED_DAILY    Return daily timestamped values across time range.
    # BREAKDOWN_DAY_OF_WEEK   Values will be returned as a breakdown by day of the week. Only valid for ACTIONS_PHONE.
    # BREAKDOWN_HOUR_OF_DAY   Values will be returned as a breakdown by hour of the day. Only valid for ACTIONS_PHONE.

    # Get start and end times dates 
    periods_metrics = get_start_and_end_time(12)

    print("get_location_report_insights_data")
    for period_metric in periods_metrics:

        # if global_parameters["gmb_insights_extraction_last_date"]:
        #     start_time = global_parameters["gmb_insights_extraction_last_date"]
        # else:
        #     start_time = period_metric.get("start_time")
        start_time = period_metric.get("start_time")
        # For each period in period metrics
        body = {
                "locationNames": [
                    location_name_address
                ],
                "basicRequest" : {
                    "metricRequests": [
                        {
                            "metric": metric,
                            "options": [
                                 options
                            ] 
                       }
                    ],
                    "timeRange": {
                        "startTime": start_time,
                        "endTime": period_metric.get("end_time"),
                    },
                }
            }
        global global_headers
        response = requests.post('https://mybusiness.googleapis.com/v4/'+account_name+'/locations:reportInsights', 
                            headers=global_headers, 
                            json=body)

        report_insights_all_metrics = response.json()
        
        try:
            len(report_insights_all_metrics.get('locationMetrics')[0].get('metricValues'))
        except Exception as e:
            print(str(e))
            continue

        for metric_value in report_insights_all_metrics.get('locationMetrics')[0].get('metricValues'):
            if not metric_value.get('metric'):
                continue 

            if metric_value.get('dimensionalValues'):
                for dimensional_value in metric_value.get('dimensionalValues'):

                    start_time_key = dimensional_value.get('timeDimension').get('timeRange').get('startTime')
                    metric_key = metric_value.get('metric').lower()
                    if start_time_key not in data[location_name_address]:
                        data[location_name_address][start_time_key] = {}

                    # Set metric 
                    try:
                        data[location_name_address][start_time_key][metric_key] = int(dimensional_value.get('value'))
                    except Exception as e:
                        # print(str(e))
                        # print(dimensional_value.get('value'))
                        data[location_name_address][start_time_key][metric_key] = 0
                    else:
                        pass

    return data

def get_driving_request_metrics_data(account_name, location_name_address):
    '''
       Get report data insights for make graphs 
    '''
    data = []
    global global_headers

    now = datetime.datetime.now().replace(hour=3, minute=0, second=0)

    num_days = "NINETY"
    # SEVEN   7 days. This is the default value.
    # THIRTY  30 days.
    # NINETY  90 days.
    
    body = {
        "locationNames": [
            location_name_address
        ],
        "drivingDirectionsRequest": { # A location indexed with the regions that people usually come from. This is captured by counting how many driving-direction requests to this location are from each region.
            "numDays": num_days # The number of days data is aggregated over.
        }   
    }

    global global_headers
    response = requests.post('https://mybusiness.googleapis.com/v4/'+account_name+'/locations:reportInsights', 
                    headers=global_headers, 
                    json=body)

    driving_request_data = response.json()
    
    # Maybe there is no data return false to next location
    try:
        driving_request_data.get('locationDrivingDirectionMetrics')[0].get('topDirectionSources')[0].get('regionCounts')
    except Exception as e:
        return False
    
    for directions_sources in driving_request_data.get('locationDrivingDirectionMetrics')[0].get('topDirectionSources')[0].get('regionCounts'):
        # {'count': '650',
        #  'label': 'São Paulo',
        #  'latlng': {'latitude': -23.5505198,
        #             'longitude': -46.6333093}
        driving_data = {
            'latitude': directions_sources.get('latlng').get('latitude'),
            'longitude': directions_sources.get('latlng').get('longitude'),
            'count': directions_sources.get('count'),
            'label': directions_sources.get('label'),
        }

        driving_data['name'] = location_name_address
        driving_data['start_time'] = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        data.append(driving_data)
    
    return data

def get_reviews_data(account_name, location_name_address):
    '''
        Get Reviews data
    '''
    data = []
    has_nextPageToken = True
    body = None
    global global_headers
    global global_parameters

    while has_nextPageToken:

        if body:
            response = requests.get('https://mybusiness.googleapis.com/v4/'+location_name_address+'/reviews', headers=global_headers, params=body) 
        else:
            response = requests.get('https://mybusiness.googleapis.com/v4/'+location_name_address+'/reviews', headers=global_headers)
    
        reviews_page = response.json()
        
        if reviews_page.get("reviews"):
            for review in reviews_page.get("reviews"):

                # 'reviews': [
                #     {'createTime': '2019-02-27T14:33:23.629Z',
                #       'name': 'accounts/115754111690128178671/locations/12580825083561213126/reviews/AIe9_BEAgUIalCkcXZnurOUpP4uArLD-86kOM5nn-lRq9s0vun89XsFHWHi3c-2MVv1w1F4oQMZsltgoZLmAviDGztWr7pKoys2aeL5ldkoM88gj_5aIdy0',
                #       'reviewId': 'AIe9_BEAgUIalCkcXZnurOUpP4uArLD-86kOM5nn-lRq9s0vun89XsFHWHi3c-2MVv1w1F4oQMZsltgoZLmAviDGztWr7pKoys2aeL5ldkoM88gj_5aIdy0',
                #       'reviewer': {'displayName': 'Leonardo Ferrari',
                #                    'profilePhotoUrl': 'https://lh3.googleusercontent.com/-wFttlWlDouM/AAAAAAAAAAI/AAAAAAAAAAA/3HxlGL5_LV0/c-rp-mo-ba3-br100/photo.jpg'},
                #       'starRating': 'FIVE',
                #       'updateTime': '2019-02-27T14:33:23.629Z'},
                #      {'comment': 'Excelente lugar, é impossvel ir no Bertioga, e '
                #                  'não gostar, tem opções para todas idades, está '
                #                  'aprovadíssimo ! !',
                #       'createTime': '2019-02-26T22:00:04.246Z',
                #       'name': 'accounts/115754111690128178671/locations/12580825083561213126/reviews/AIe9_BFmRFRFwJGRfUDOW8jG3rXnWMONjL-N1tICiZHRa0jtJwODRThPnU1kcmPZiC5iLTQnK9ESEl6cOygsgGfn_joQ1JJmIgcO3GkT7yn-zCiwwJawVXE',
                #       'reviewId': 'AIe9_BFmRFRFwJGRfUDOW8jG3rXnWMONjL-N1tICiZHRa0jtJwODRThPnU1kcmPZiC5iLTQnK9ESEl6cOygsgGfn_joQ1JJmIgcO3GkT7yn-zCiwwJawVXE',
                #       'reviewer': {'displayName': 'Jair Soares',
                #                    'profilePhotoUrl': 'https://lh5.googleusercontent.com/-2VV9sZnOZas/AAAAAAAAAAI/AAAAAAAAAAA/6ll2Guh9kaI/c-rp-mo-ba4-br100/photo.jpg'},
                #       'starRating': 'FIVE',
                #       'updateTime': '2019-02-26T22:00:04.246Z'},
                if review.get('createTime') <= global_parameters["gmb_reviews_extraction_last_date"]:
                    continue

                review_data = {
                    'name': location_name_address,
                    'review_name': review.get('reviewId'),
                    'display_name': review.get('reviewer').get('displayName'),
                    'profile_photo_url': review.get('reviewer').get('profilePhotoUrl'),
                    'star_rating': review.get('starRating')
                }
                
                if review.get('comment'):
                    review_data['text_en'] = get_text(review.get('comment'),"en")
                    review_data['text_pt_br'] = get_text(review.get('comment'),"pt_br")
                else:
                    review_data['text_en'] = ''
                    review_data['text_pt_br'] = ''

                review_data['comment'] = review.get('comment')
                review_data['update_time'] = review.get('updateTime')
                review_data['create_time'] = review.get('createTime')
                if review.get('starRating') == "FIVE":
                    review_data['star_rating_int'] = 5

                elif review.get('starRating') == "FOUR":
                    review_data['star_rating_int'] = 4

                elif review.get('starRating') == "THREE":
                    review_data['star_rating_int'] = 3

                elif review.get('starRating') == "TWO":
                    review_data['star_rating_int'] = 2

                elif review.get('starRating') == "ONE":
                    review_data['star_rating_int'] = 1

                data.append(review_data)

        if not reviews_page.get("nextPageToken"):
            has_nextPageToken = False
        else:
            body = {"pageToken": reviews_page.get("nextPageToken")}

    return data

def get_questions_data(account_name, location_name_address):
    
    has_nextPageToken = True
    body = None
    global global_headers

    questions_data = []
    answers_data = []
    
    # AuthorType
    # AUTHOR_TYPE_UNSPECIFIED This should not be used.
    # REGULAR_USER    A regular user.
    # LOCAL_GUIDE A Local Guide
    # MERCHANT    The owner/manager of the location
    while has_nextPageToken:

        if body:
            response = requests.get('https://mybusiness.googleapis.com/v4/'+location_name_address+'/questions', headers=global_headers, params=body) 
        else:
            response = requests.get('https://mybusiness.googleapis.com/v4/'+location_name_address+'/questions', headers=global_headers) 
    
        questions_page = json.loads(response.text)
        
        if questions_page.get("questions"):
            for question in questions_page.get("questions"):

                #
                # Questions
                #
                # "author": {
                #     "displayName": "",
                #     "profilePhotoUrl": "//lh4.ggpht.com/-ox9XEh95V3o/AAAAAAAAAAI/AAAAAAAAAAA/hGV91x1ligs/c0x00000000-cc-rp-mo/photo.jpg",
                #     "type": "REGULAR_USER"
                #   },
                #   "createTime": "2019-10-20T13:18:32.211208Z",
                #   "name": "accounts/115754111690128178671/locations/12580825083561213126/questions/AIe9_BH-Y7Lwr6IxuH7aUb2G6RG2I7Zd58YBfNwtTqShf9-YUz_1SIBS3p5OOUOWr5L7c9TEfDcaHJ5NSeqL5Ox4R5ImezQf0vczVct5u4cvYRAEQt-22iWNAfGI--w6Sy1YCiwDANey",
                #   "text": "Bom dia,Como eu posso conhecer o  Bertioga ",

                data = {
                    'name': location_name_address,
                    'question_name': question.get('name'),
                    'display_name': question.get('author').get('displayName'),
                    'profile_photo_url': question.get('author').get('profilePhotoUrl'),
                    'type': question.get('author').get('type'),
                    'text_pt_br': get_text(question.get('text'), 'pt_br'),
                    'text_en': get_text(question.get('text'), 'en'),
                    'text': question.get('text'),
                    'create_time': question.get('createTime'),
                }
                questions_data.append(data)

                if question.get('topAnswers'):
                    for answer in question.get('topAnswers'):

                        # "author": {
                        #     "displayName": "César Morais",
                        #     "profilePhotoUrl": "//lh3.ggpht.com/-RYMDqop1oN0/AAAAAAAAAAI/AAAAAAAAAAA/U1EQRLw-99A/c0x00000000-cc-rp-mo-ba3/photo.jpg",
                        #     "type": "LOCAL_GUIDE"
                        #   },
                        #   "createTime": "2019-10-20T14:47:15.273260Z",
                        #   "name": "accounts/115754111690128178671/locations/12580825083561213126/questions/AIe9_BH-Y7Lwr6IxuH7aUb2G6RG2I7Zd58YBfNwtTqShf9-YUz_1SIBS3p5OOUOWr5L7c9TEfDcaHJ5NSeqL5Ox4R5ImezQf0vczVct5u4cvYRAEQt-22iWNAfGI--w6Sy1YCiwDANey/answers/AIe9_BFu3rdicGrPrzdyu4PDXmqMuEM7Ezl_u8drbRWQo-LLUBAgghpt685nDfOxNa38mUhthKysTf5UDqT7lY6pK9TTRbukvFZL9uMpUzH4MpAlVIXoBhQOjZtYdS660i8U-XWl9yc6XGED-DWbw_2wigi_Lo99EP02rxuEzKeqNJZO7tW8ZYufrydFmMQ38uvEa3IwUO_-",
                        #   "text": "Você precisa ter a carteirinha do. Pode verificar pelo site centrodeferiassp.org.br\nLá aparecem as opções para inscrição em viagens daqui a 6 meses (dependerá ainda de um sorteio) ou vagas de desistentes.\nOu então pode comparecer no mais próximo e pegar informações de excursões para passar 1 dia no Bertioga.\nEspero ter ajudado.",
                        #   "updateTime": "2019-10-20T14:47:15.273260Z"

                        data = {
                            'name': location_name_address,
                            'question_name': question.get('name'),
                            'answers_name': answer.get('name'),
                            'display_name': answer.get('author').get('displayName'),
                            'profile_photo_url': answer.get('author').get('profilePhotoUrl'),
                            'type': answer.get('author').get('type'),
                            'text_pt_br': get_text(answer.get('text'), 'pt_br'),
                            'text_en': get_text(answer.get('text'), 'en'),
                            'text': answer.get('text'),
                            'create_time': answer.get('createTime'),
                            'update_time': answer.get('updateTime'),
                            'top_answer': True
                        }
                        answers_data.append(data)

        if not questions_page.get("nextPageToken"):
            has_nextPageToken = False
        else:
            body = {"pageToken": questions_page.get("nextPageToken")}

    return {"questions_data": questions_data, 
            "answers_data": answers_data}

# def get_answers_data(account_name, location_name_address):

#     # {
#     #   "name": string,
#     #   "author": {
#     #     object (Author)
#     #   },
#     #   "upvoteCount": number,
#     #   "text": string,
#     #   "createTime": string,
#     #   "updateTime": string
#     # }
#     return None

def get_words_count(conn, account_name, location_name_address, entity):
    '''
        Process get words from entities (reviews,questions,answers)
    '''
    all_words = {}
    # data = {} 
    kill_punctuation = str.maketrans('', '', r"-()\"#/@;:<>{}-=~|.?,")

    if entity == "reviews":
        query = 'SELECT "name", review_name, comment, text_pt_br, create_time from '+entity+' WHERE has_words_count_processed = FALSE OR has_words_count_processed IS NULL'
    elif entity == "questions":
        query = 'SELECT "name", question_name, text_pt_br, text_en, create_time, "type" from '+entity+' WHERE has_words_count_processed = FALSE OR has_words_count_processed IS NULL'
    elif entity == "answers":
        query = 'SELECT "name", question_name, text_pt_br, text_en, create_time, "type" from '+entity+' WHERE has_words_count_processed = FALSE OR has_words_count_processed IS NULL'
    
    cur = conn.cursor(cursor_factory = psycopg2.extras.DictCursor) 
    cur.execute(query)
    results = cur.fetchall()

    for line in results:
        if entity == "reviews":
            line[2] = get_text(line[2], 'pt_br').lower()
        
        line.append([])
        for word in line[2].split(' '):
            word = word.translate(kill_punctuation)
            if word == "empty":
                continue

            if len(word) <= 2:
                continue

            if word == "not implemented":
                continue             
            
            if word == "not":
                continue                
            
            if word == "implemented":
                continue                

            line[len(line)-1].append(word)    
    
    return results 

def update_control_field_aws_compreend(table_name, detect_type):
    print(table_name)
    print(detect_type)

    if detect_type == "entities":
        aws_table = 'aws_compreend_entities'
        field = 'has_aws_compreend_name_entity_processed'
    elif detect_type == "key_phrase":
        aws_table = 'aws_compreend_key_phrase'
        field = 'has_aws_compreend_key_phrase_processed'
    elif detect_type == "sentiment":
        aws_table = 'aws_compreend_sentiment'
        field = 'has_aws_compreend_sentiment_processed'

    update_statment = 'UPDATE "' + table_name + '" SET ' + field + ' = TRUE ' + \
        'WHERE (SELECT COUNT(*) ' + \
        'FROM "' + aws_table + '"" ' + \
        'WHERE origin_entity = "' + table_name +'" ' + \
        '   AND origin_id = "' + table_name + '".id ) > 0; '

    try:
        global global_conn
        global_conn.cursor().execute(update_statment)
    except Exception as e:
        print(str(e))
        pass
    else:
        print("aws compreend control field updated: " + field + " " + aws_table + " in " + table_name)
        cur.execute('COMMIT;')
    return

def process_aws_compreend(detect_type, text):
    '''
        entity_table (reviews, questions, answers)
        detect_type (sentiment, key_phrase, entities)
    '''
    
    comprehend = boto3.client(service_name='comprehend', region_name='us-east-1')

    if detect_type == "entity":
        # {
        #     "Entities": [
        #         {
        #             "Text": "today",
        #             "Score": 0.97,
        #             "Type": "DATE",
        #             "BeginOffset": 14,
        #             "EndOffset": 19
        #         },
        #         {
        #             "Text": "Seattle",
        #             "Score": 0.95,
        #             "Type": "LOCATION",
        #             "BeginOffset": 23,
        #             "EndOffset": 30
        #         }
        #     ],
        #     "LanguageCode": "en"
        # }
        
        data = comprehend.detect_entities(Text=text, LanguageCode='en')

        try:
            data = []

            return_compreend = comprehend.detect_key_phrases(Text=text, LanguageCode='en')
            print(str(return_compreend))

            if return_compreend.get('ResponseMetadata').get('HTTPStatusCode') == 200:

                entities = return_compreend.get('Entities')
                
                for entity in entities:
                    data_dict = {}

                    data_dict["score"] = entity.get('Score')
                    data_dict["type"] = entity.get('Type')
                    data_dict["text"] = entity.get('Text')
                    data_dict["begin_offset"] = entity.get('BeginOffset')
                    data_dict["end_offset"] = entity.get('EndOffset')

                    data.append(data_dict)

        except Exception as e:
            print(str(e))
            return []
        
        else:
            return data

    # elif detect_type == "key_phrase":
        
        # print(json.dumps(comprehend.detect_key_phrases(Text=text, LanguageCode='en'), sort_keys=True, indent=4))
        # {
        #     "LanguageCode": "en",
        #     "KeyPhrases": [
        #         {
        #             "Text": "today",
        #             "Score": 0.89,
        #             "BeginOffset": 14,
        #             "EndOffset": 19
        #         },
        #         {
        #             "Text": "Seattle",
        #             "Score": 0.91,
        #             "BeginOffset": 23,
        #             "EndOffset": 30
        #         }
        #     ]
        # }
        # try:
        #     data = []

        #     return_compreend = comprehend.detect_key_phrases(Text=text, LanguageCode='en')
        #     print(str(return_compreend))

        #     if return_compreend.get('ResponseMetadata').get('HTTPStatusCode') == 200:

        #         key_phrases = return_compreend.get('KeyPhrases')
                
        #         for key_phrase in key_phrases:
        #             data_dict = {}

        #             data_dict["score"] = key_phrase.get('Score')
        #             data_dict["text"] = key_phrase.get('Text')
        #             data_dict["begin_offset"] = key_phrase.get('BeginOffset')
        #             data_dict["end_offset"] = key_phrase.get('EndOffset')

        #             data.append(data_dict)

        # except Exception as e:
        #     print(str(e))
        #     return []
        
        # else:
        #     return data

    elif detect_type == "sentiment":
        # {
        #     "SentimentScore": {
        #         "Mixed": 0.014585512690246105,
        #         "Positive": 0.31592071056365967,
        #         "Neutral": 0.5985543131828308,
        #         "Negative": 0.07093945890665054
        #     },
        #     "Sentiment": "NEUTRAL",
        #     "LanguageCode": "en"
        # }
        try:
            data = {}

            return_compreend = comprehend.detect_sentiment(Text=text, LanguageCode='en')
            print(str(return_compreend))
            if return_compreend.get('ResponseMetadata').get('HTTPStatusCode') == 200:
                data["sentiment"] = return_compreend.get('Sentiment')
                data["positive"] = return_compreend.get('SentimentScore').get('Positive')
                data["neutral"] = return_compreend.get('SentimentScore').get('Neutral')
                data["negative"] = return_compreend.get('SentimentScore').get('Negative')
                data["mixed"] = return_compreend.get('SentimentScore').get('Mixed')
        except Exception as e:
            print(str(e))
            return {}
        
        else:
            print(data)
            return data
    return

def get_text_from(conn, text_entity, aws_compreend_operation):
    '''
        text_entity (reviews,questions,answers)

    '''
    aws_where_query = ''

    if aws_compreend_operation == "sentiment":
        aws_where_query = "has_aws_compreend_sentiment_processed = FALSE AND text_en IS NOT NULL AND text_en > '' "
    
    elif aws_compreend_operation == "key_phrase":
        aws_where_query = "has_aws_compreend_name_entity_processed = FALSE AND text_en IS NOT NULL AND text_en > '' "
    
    elif aws_compreend_operation == "entities":
        aws_where_query = "has_aws_compreend_key_phrase_processed = FALSE AND text_en IS NOT NULL AND text_en > '' "

    query = 'SELECT id, text_en FROM ' + text_entity + ' WHERE ' + aws_where_query

    cur = conn.cursor(cursor_factory = psycopg2.extras.DictCursor) 
    cur.execute(query)
    results = cur.fetchall()

    # return {"keys": cur.index, "results": results}
    return results

def manage_parameter(operation, parameter = None, value = None):
    '''
        Handle Parameters
    '''
    # global global_conn
    # global global_parameters

    def set_parameter(parameter, value):

        if not value:
            now = datetime.datetime.now()
            update_time = (now.replace(day=1, hour=0,minute=0,second=0, microsecond=0) - datedelta.datedelta(months=0))
            value = update_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        update_statment = 'UPDATE public.base_parameters SET "value" = ' + value + ' WHERE "name" = ' + parameter

        try:
            global_conn.cursor().execute(update_statment)
        except Exception as e:
            print(str(e))
            pass
        else:
            print("parameter: " + parameter + " updated")
            cur.execute('COMMIT;')
        return

    def get_parameters():
        return_dict = {}
        query = 'SELECT * FROM public.base_parameters'

        cur = global_conn.cursor(cursor_factory = psycopg2.extras.DictCursor) 
        cur.execute(query)
        results = cur.fetchall()
        
        for result in results:
            return_dict.update({result["name"]: result["value"]})

        return return_dict

    if operation == "get":
        global global_parameters
        global_parameters = get_parameters()
        print(str(global_parameters))
    else: 
        set_parameter(parameter, value) 


def gmb_extract_data(conn, event):
    ''' 
        main function 
    '''
    print("GMB API | start ")
    print("GMB API | authenticating... ")
    
    # Make authorized API calls by using OAuth 2.0 client ID credentials for authentication & authorization
    parser = argparse.ArgumentParser(parents=[tools.argparser])
    flags = parser.parse_args()
    flow = flow_from_clientsecrets('client_secrets.json',
        scope='https://www.googleapis.com/auth/plus.business.manage',
        redirect_uri='http://localhost:8080/')

    # For retrieving the refresh token
    flow.params['access_type'] = 'offline'
    flow.params['approval_prompt'] = 'force' 

    # Use a Storage in current directory to store the credentials in
    # storage = Storage('.' + os.path.basename(__file__)) # ipdb does not have access __file__ storage = Storage('credentials')
    storage = Storage('credentials')
    credentials = storage.get()

    # credentials = tools.run_flow(flow, storage, flags) 
    # storage.put(credentials)
    # Acquire credentials in a command-line application
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage, flags) 
        storage.put(credentials)

    # Apply necessary credential headers to all requests made by an httplib2.Http instance
    http = credentials.authorize(httplib2.Http())

    # Refresh the access token if it expired
    if credentials is not None and credentials.access_token_expired:
        try:
            credentials.refresh(http)
            storage.put(credentials)
        except:
            pass

    access_token = credentials.token_response["access_token"]
    # print(f"Access token:{access_token}")

    headers = {
        'authorization': "Bearer " + access_token,
        'content-type': "application/json",
    }

    # Set Global Header
    global global_parameters
    global global_headers
    global_headers = headers

    # Set Global Header
    global global_conn
    global_conn = conn

    print("GMB API | authentication complete")
    print("Listing accounts..")
    url = 'https://mybusiness.googleapis.com/v4/accounts'
    response = requests.get(url, headers=global_headers)

    accounts_dict = json.loads(response.text)
    account_name = accounts_dict.get('accounts')[0].get('name')
    print("Listing locations..")

    url = 'https://mybusiness.googleapis.com/v4/'+account_name+'/locations'
    response = requests.get(url, headers=global_headers)
    locations = json.loads(response.text)

    # Load Pameters
    manage_parameter("get")
    print(str(global_parameters))

    for location in locations.get('locations'):
        
        # Set some variables
        location_name_address = location.get('name')

        # Set dict from location for insert operation
        data = {
            'name': location.get('name'),
            'location_name': location.get('locationName'),
            'primary_phone': location.get('primaryPhone'),
            'address_lines': location.get('address').get('addressLines')[0],
            'language_code': location.get('address').get('languageCode'),
            'administrative_area': location.get('address').get('administrativeArea'),
            'locality': location.get('address').get('locality'),
            'postal_code': location.get('address').get('postalCode'),
            'region_code': location.get('address').get('regionCode'),
            'sub_locality': location.get('address').get('sublocality'),
            'category_id': location.get('primaryCategory').get('categoryId'),
            'display_name': location.get('primaryCategory').get('displayName'),
        }

        if location.get('latlng'):
            # Because Sometimes latlng is not avaliable for some locations
            data["latitude"] = location.get('latlng').get('latitude')
            data["longitude"] = location.get('latlng').get('latitude')

        # Insert Data for location
        format_and_insert_data(conn, 'gmb_location', data)
        
        if eval(event["extract_gmb_data"]):
            print("extract_gmb_data: on")
            # If extract GMB mode is turned on

            # get_location_report_insights_data
            data = get_location_metrics_data(account_name, location_name_address)
            format_and_insert_data(conn, 'gmb_location_metrics', data)

            # get_location_report_insights_data
            # data = get_driving_request_metrics_data(account_name, location_name_address)
            # if data:
            #     format_and_insert_data(conn, 'gmb_driving_request_metrics', data)

            # # get_reviews
            # locations_names = ['accounts/113835407260154410434/locations/13004285850978662066',
            # 'accounts/113835407260154410434/locations/4459407390914496748',
            # 'accounts/113835407260154410434/locations/16442906952516165115',
            # 'accounts/113835407260154410434/locations/12580697724909839464',
            # 'accounts/113835407260154410434/locations/15269809438244576126',
            # 'accounts/113835407260154410434/locations/9017677128479394582',
            # 'accounts/113835407260154410434/locations/13870810297318349927',
            # 'accounts/113835407260154410434/locations/14753913830020813103',
            # 'accounts/113835407260154410434/locations/11259572150200371765',
            # 'accounts/113835407260154410434/locations/7565655238609991473',
            # 'accounts/113835407260154410434/locations/9427559072449490224',
            # 'accounts/113835407260154410434/locations/7971926267197127795',
            # 'accounts/113835407260154410434/locations/13048619174309703721',
            # 'accounts/113835407260154410434/locations/14186626403355399547',
            # 'accounts/113835407260154410434/locations/11205071920113794687',
            # 'accounts/113835407260154410434/locations/18427472591937509150',
            # 'accounts/113835407260154410434/locations/2239325776209898608',
            # 'accounts/113835407260154410434/locations/2933186324193168388',
            # 'accounts/113835407260154410434/locations/15355904013498222406',
            # 'accounts/113835407260154410434/locations/11265182408813184425',
            # 'accounts/113835407260154410434/locations/14811789727235575022',
            # 'accounts/113835407260154410434/locations/8019909962414801034',
            # 'accounts/113835407260154410434/locations/14871576153298904937',
            # 'accounts/113835407260154410434/locations/11726226333069235428',
            # 'accounts/113835407260154410434/locations/10994105873111054134',
            # 'accounts/113835407260154410434/locations/12580825083561213126',
            # 'accounts/113835407260154410434/locations/4385129717904718738']

            # if location_name_address not in locations_names:
            #     data = get_reviews_data(account_name, location_name_address)
            #     if data:
            #         format_and_insert_data(conn, 'gmb_reviews', data)

            # # # get_questions
            # data = get_questions_data(account_name, location_name_address)
            # if data:
            #     format_and_insert_data(conn, 'gmb_questions', data.get('questions_data'))
            #     format_and_insert_data(conn, 'gmb_answers', data.get('answers_data'))

            # get_answers
            # @TODO: montar answers
            # data = get_answers_data(account_name, location_name_address)
            # format_and_insert_data(conn, 'answers', data)
        
        # 
        # 
        # 
        # get_words_count
        # 
        # 
        # 
        if eval(event["process_words_count"]):
            print("process_words_count: on")

            data = get_words_count(conn, account_name, location_name_address, "reviews")
            if data:
                format_and_insert_data(conn, 'words_count', data, "reviews")
            data = get_words_count(conn, account_name, location_name_address, "questions")
            if data:
                format_and_insert_data(conn, 'words_count', data, "questions")
            data = get_words_count(conn, account_name, location_name_address, "answers")
            if data:
                format_and_insert_data(conn, 'words_count', data, "answers")

        #
        #
        # process_aws_compreend
        #
        #
        if eval(event["process_aws_comprehend"]):
            # @TODO: Terminar o restante
            # aws_compreend_operations = ["sentiment", "entities", "key_phrase"]
            # entities = ["reviews", "questions", "answers"]
            print("process_aws_comprehend: on")

            aws_compreend_operations = ["sentiment"]
            entities = ["gmb_reviews"]
            for entity in entities:
                print(entity)
                for aws_compreend_operation in aws_compreend_operations:
                    print(aws_compreend_operation)
                    data = get_text_from(conn, entity, aws_compreend_operation)
                    
                    for row in data:
                        aws_data = process_aws_compreend(aws_compreend_operation, row["text_en"])

                        if aws_data:

                            if aws_compreend_operation == "sentiment":
                                data_dict = {}
                                data_dict["origin_entity"] = entity
                                data_dict["origin_id"] = row["id"]
                                data_dict["sentiment"] = aws_data["sentiment"]
                                data_dict["mixed"] = aws_data["mixed"]
                                data_dict["positive"] = aws_data["positive"]
                                data_dict["neutral"] = aws_data["neutral"]
                                data_dict["negative"] = aws_data["negative"]

                                print(str(data_dict))
                                format_and_insert_data(conn, 'aws_compreend_sentiment', data_dict)
                            elif aws_compreend_operation == "entities":
                                data = []
                                for entity in aws_data:
                                
                                    data_dict = {}
                                    data_dict["origin_entity"] = entity
                                    data_dict["origin_id"] = row["id"]
                                    data_dict["type"] = entity["type"]
                                    data_dict["score"] = entity["score"]
                                    data_dict["text"] = entity["text"]
                                    data_dict["begin_offset"] = entity["begin_offset"]
                                    data_dict["end_offset"] = entity["end_offset"]

                                    data.append(data_dict)
                                format_and_insert_data(conn, 'aws_compreend_entities', data)

                            elif aws_compreend_operation == "key_phrase":
                                data = []
                                for key_phrase in aws_data:
                                
                                    data_dict = {}
                                    data_dict["origin_entity"] = entity
                                    data_dict["origin_id"] = row["id"]
                                    data_dict["score"] = key_phrase["score"]
                                    data_dict["text"] = key_phrase["text"]
                                    data_dict["begin_offset"] = key_phrase["begin_offset"]
                                    data_dict["end_offset"] = key_phrase["end_offset"]

                                    data.append(data_dict)
                                format_and_insert_data(conn, 'aws_compreend_key_phrase', data)

                    update_control_field_aws_compreend(entity, aws_compreend_operation)
