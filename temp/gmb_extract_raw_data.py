import os, httplib2, json, argparse
# import xlsxwriter
# import pprint
import requests
import json
import time
import sys
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client import tools
from googleapiclient.discovery import build
from apiclient import errors

print("GMB API | start ")

print("GMB API | authenticating... ")
# Make authorized API calls by using OAuth 2.0 client ID credentials for authentication & authorization
parser = argparse.ArgumentParser(parents=[tools.argparser])
flags = parser.parse_args()
flow = flow_from_clientsecrets('client_secrets.json',
    scope='https://www.googleapis.com/auth/plus.business.manage',
    redirect_uri='https://localhost:8080')

# For retrieving the refresh token
flow.params['access_type'] = 'offline'
flow.params['approval_prompt'] = 'force' 

# Use a Storage in current directory to store the credentials in
storage = Storage('.' + os.path.basename(__file__))
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
print("GMB API | authentication complete")

print("Listing accounts..")
url = 'https://mybusiness.googleapis.com/v4/accounts'
response = requests.get(url, headers=headers)
# print(f"Status code: {response.status_code}")
# print(f"Status text: {response.text}")

accounts_dict = json.loads(response.text)
account_name = accounts_dict.get('accounts')[0].get('name')
print("")
print("Listing locations..")

# accounts_dict.get('accounts')[0].get('name')
# 'accounts/115754111690128178671'

# TODO: Listar locations por accounts (iterable)
url = 'https://mybusiness.googleapis.com/v4/'+account_name+'/locations'
response = requests.get(url, headers=headers)
locations = json.loads(response.text)

# Remove a file
excel_file = "/Users/fabio/Downloads/output_gmb_data.xlsx"
try:
    os.remove(excel_file)
except Exception as e:
    print(e)
else:
   workbook = xlsxwriter.Workbook(r''+excel_file)
# workbook = xlsxwriter.Workbook(r''+excel_file)

row = 1
col = 0

# Worksheet principal
worksheet_principal = workbook.add_worksheet('Principal')
worksheet_location_metric = workbook.add_worksheet('Location Metrics')
worksheet_driving_request_metric = workbook.add_worksheet('Driving Request Metrics')
worksheet_reviews = workbook.add_worksheet('Reviews')
worksheet_questions = workbook.add_worksheet('Questions')
worksheet_answers = workbook.add_worksheet('Answers')

# Header principal
worksheet_principal.write(0, col, 'name')
col = col + 1
worksheet_principal.write(0, col, 'locationName')
col = col + 1
worksheet_principal.write(0, col, 'categoryId')
col = col + 1
worksheet_principal.write(0, col, 'displayName')
col = col + 1
worksheet_principal.write(0, col, 'latitude')
col = col + 1
worksheet_principal.write(0, col, 'longitude')
col = col + 1
worksheet_principal.write(0, col, 'addressLines')
col = col + 1
worksheet_principal.write(0, col, 'administrativeArea')
col = col + 1
worksheet_principal.write(0, col, 'languageCode')
col = col + 1
worksheet_principal.write(0, col, 'locality')
col = col + 1
worksheet_principal.write(0, col, 'postalCode')
col = col + 1
worksheet_principal.write(0, col, 'regionCode')
col = col + 1
worksheet_principal.write(0, col, 'sublocality')
col = col + 1

periods_metrics = [
    {
        "start_time": "2019-07-01T01:01:23.045123456Z",
        "end_time": "2019-07-31T23:59:59.045123456Z"
    },
    {
        "start_time": "2019-08-01T01:01:23.045123456Z",
        "end_time": "2019-08-31T23:59:59.045123456Z"
    },
    {
        "start_time": "2019-09-01T01:01:23.045123456Z",
        "end_time": "2019-09-30T23:59:59.045123456Z"
    },
]

# Location Metric
# https://mybusiness.googleapis.com/v4/accounts/115754111690128178671/locations/12580825083561213126/report:Insights
col = 0 
worksheet_location_metric.write(0, col, 'name')
col = col + 1
worksheet_location_metric.write(0, col, 'start_time')
col = col + 1
worksheet_location_metric.write(0, col, 'end_time')
col = col + 1
worksheet_location_metric.write(0, col, 'QUERIES_DIRECT')
col = col + 1
worksheet_location_metric.write(0, col, 'QUERIES_INDIRECT')
col = col + 1
worksheet_location_metric.write(0, col, 'QUERIES_CHAIN')
col = col + 1
worksheet_location_metric.write(0, col, 'VIEWS_MAPS')
col = col + 1
worksheet_location_metric.write(0, col, 'VIEWS_SEARCH')
col = col + 1
worksheet_location_metric.write(0, col, 'ACTIONS_WEBSITE')
col = col + 1
worksheet_location_metric.write(0, col, 'ACTIONS_PHONE')
col = col + 1
worksheet_location_metric.write(0, col, 'ACTIONS_DRIVING_DIRECTIONS')
col = col + 1
worksheet_location_metric.write(0, col, 'PHOTOS_VIEWS_MERCHANT')
col = col + 1
worksheet_location_metric.write(0, col, 'PHOTOS_VIEWS_CUSTOMERS')
col = col + 1
worksheet_location_metric.write(0, col, 'PHOTOS_COUNT_MERCHANT')
col = col + 1
worksheet_location_metric.write(0, col, 'PHOTOS_COUNT_CUSTOMERS')
col = col + 1
worksheet_location_metric.write(0, col, 'LOCAL_POST_VIEWS_SEARCH')
col = col + 1
worksheet_location_metric.write(0, col, 'LOCAL_POST_ACTIONS_CALL_TO_ACTION')
col = col + 1

# Driving Request Metric
# https://mybusiness.googleapis.com/v4/accounts/115754111690128178671/locations/12580825083561213126/report:Insights
col = 0
worksheet_driving_request_metric.write(0, col, 'name')
col = col + 1
worksheet_driving_request_metric.write(0, col, 'dayCount')
col = col + 1
worksheet_driving_request_metric.write(0, col, 'latitude')
col = col + 1
worksheet_driving_request_metric.write(0, col, 'longitude')
col = col + 1
worksheet_driving_request_metric.write(0, col, 'label')
col = col + 1
worksheet_driving_request_metric.write(0, col, 'count')
col = col + 1

# Reviews
# https://mybusiness.googleapis.com/v4/accounts/115754111690128178671/locations/12580825083561213126/reviews
col = 0
worksheet_reviews.write(0, col, "name")
col = col + 1
worksheet_reviews.write(0, col, "displayName")
col = col + 1
worksheet_reviews.write(0, col, "profilePhotoUrl")
col = col + 1
worksheet_reviews.write(0, col, "starRating")
col = col + 1
worksheet_reviews.write(0, col, "comment")
col = col + 1
worksheet_reviews.write(0, col, "updateTime")
col = col + 1
worksheet_reviews.write(0, col, "createTime")
col = col + 1

# Questions
# https://mybusiness.googleapis.com/v4/accounts/115754111690128178671/locations/12580825083561213126/questions
col = 0
worksheet_questions.write(0, col, "name")
col = col + 1
worksheet_questions.write(0, col, "question_name")
col = col + 1
worksheet_questions.write(0, col, "displayName")
col = col + 1
worksheet_questions.write(0, col, "profilePhotoUrl")
col = col + 1
worksheet_questions.write(0, col, "type")
col = col + 1
worksheet_questions.write(0, col, "text")
col = col + 1
worksheet_questions.write(0, col, "createTime")
col = col + 1

# Answers
# https://mybusiness.googleapis.com/v4/accounts/115754111690128178671/locations/12580825083561213126/answers
col = 0
worksheet_answers.write(0, col, "name")
col = col + 1
worksheet_answers.write(0, col, "question_name")
col = col + 1
worksheet_answers.write(0, col, "displayName")
col = col + 1
worksheet_answers.write(0, col, "profilePhotoUrl")
col = col + 1
worksheet_answers.write(0, col, "type")
col = col + 1
worksheet_answers.write(0, col, "text")
col = col + 1
worksheet_answers.write(0, col, "createTime")
col = col + 1
worksheet_answers.write(0, col, "updateTime")
col = col + 1

# Number Line definitions
main_line = 1
location_metric_line = 1
driving_metric_line = 1
review_line = 1
question_line = 1
answers_line = 1

for location in locations.get('locations'):

    if location.get('name') in "accounts/115754111690128178671/locations/12580825083561213126" or location.get('name') in "accounts/115754111690128178671/locations/2933186324193168388":
        print(location.get('name'))
    else:
        continue

    col = 0
    for key in location.keys():
        # ['name',
        # 'storeCode',
        # 'locationName',
        # 'primaryPhone',
        # 'primaryCategory',
        # 'websiteUrl',
        # 'regularHours',
        # 'locationKey',
        # 'latlng',
        # 'openInfo',
        # 'locationState',
        # 'metadata',
        # 'languageCode',
        # 'address'])
        
        if key == 'name':
            # 'name': 'accounts/115754111690128178671/locations/12580825083561213126',
            worksheet_principal.write(main_line, col, location.get(key)) # : 'accounts/115754111690128178671/locations/12580825083561213126',,
            col = col + 1

        elif key == 'locationName':
            # 'locationName': ' Bertioga',
            worksheet_principal.write(main_line, col, location.get(key)) # : 'SP',
            col = col + 1

        elif key == 'primaryCategory':
            # 'primaryCategory': {'categoryId': 'gcid:social_services_organization',
            #                    'displayName': 'Organização de serviço social'},
            worksheet_principal.write(main_line, col, location.get(key).get('categoryId'))
            col = col + 1
            worksheet_principal.write(main_line, col, location.get(key).get('displayName'))
            col = col + 1           

        elif key == 'address':
            # {'addressLines': ['Rua Pastor Djalma da Silva Coimbra, 20'],
            #  'administrativeArea': 'SP',
            #  'languageCode': 'pt',
            #  'locality': 'Bertioga',
            #  'postalCode': '11250-000',
            #  'regionCode': 'BR',
            #  'sublocality': 'Jardim Rio da Praia'},
            worksheet_principal.write(main_line, col, location.get(key).get('addressLines')[0]) # TODO: ['Rua Pastor Djalma da Silva Coimbra, 20'],
            col = col + 1
            worksheet_principal.write(main_line, col, location.get(key).get('administrativeArea')) # : 'SP',
            col = col + 1
            worksheet_principal.write(main_line, col, location.get(key).get('languageCode')) # : 'pt',
            col = col + 1
            worksheet_principal.write(main_line, col, location.get(key).get('locality')) # : 'Bertioga',
            col = col + 1
            worksheet_principal.write(main_line, col, location.get(key).get('postalCode')) # : '11250-000',
            col = col + 1
            worksheet_principal.write(main_line, col, location.get(key).get('regionCode')) # : 'BR',
            col = col + 1
            worksheet_principal.write(main_line, col, location.get(key).get('sublocality')) # : 'Jardim Rio da Praia'},
            col = col + 1

        elif key == 'latlng':
            # 'latlng': {
            #       'latitude': -23.8276293, 
            #       'longitude': -46.1134884
            # },
            worksheet_principal.write(main_line, col, location.get(key).get('latitude')) # 'latitude': -23.8276293, 
            col = col + 1
            worksheet_principal.write(main_line, col, location.get(key).get('longitude')) # 'longitude': -46.1134884
            col = col + 1
        
    main_line = main_line + 1

    #
    # location metrics 
    #
    # 
    # 
    print("Listing location: ")
    print("")
    print(location.get('locationName'))
    print("")

    # 'name': 'accounts/115754111690128178671/locations/12580825083561213126'
    location_name = location.get('locationName')
    location_name_address = location.get('name')

    # start_time = "2019-09-01T01:01:23.045123456Z"
    # end_time = "2019-09-30T23:59:59.045123456Z"

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

    options = "AGGREGATED_TOTAL"
    # METRIC_OPTION_UNSPECIFIED   No metric option specified. Will default to AGGREGATED_TOTAL in a request.
    # AGGREGATED_TOTAL    Return values aggregated over the entire time frame. This is the default value.
    # AGGREGATED_DAILY    Return daily timestamped values across time range.
    # BREAKDOWN_DAY_OF_WEEK   Values will be returned as a breakdown by day of the week. Only valid for ACTIONS_PHONE.
    # BREAKDOWN_HOUR_OF_DAY   Values will be returned as a breakdown by hour of the day. Only valid for ACTIONS_PHONE.

    num_days = 7
    # SEVEN   7 days. This is the default value.
    # THIRTY  30 days.
    # NINETY  90 days.

    print("")
    
    for period in periods_metrics:
        col = 0
        
        print("Listing Report Insights start_time: "+period.get("start_time")+" - end_date: "+period.get("end_time")+" | location metrics ")
        
        worksheet_location_metric.write(location_metric_line, col, location_name_address)
        col = col + 1
        worksheet_location_metric.write(location_metric_line, col, period.get("start_time"))
        col = col + 1
        worksheet_location_metric.write(location_metric_line, col, period.get("end_time"))
        col = col + 1

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
                    "startTime": period.get("start_time"),
                    "endTime": period.get("end_time"),
                },
            }
        }

        print(location_name_address)
        response = requests.post('https://mybusiness.googleapis.com/v4/'+account_name+'/locations:reportInsights', 
                            headers=headers, 
                            json=body) 
        
        report_insights_all_metrics = json.loads(response.text)
        for metric_value in report_insights_all_metrics.get('locationMetrics')[0].get('metricValues'):

            #       [{'metric': 'QUERIES_DIRECT',
            #         'totalValue': { 
            #              'metricOption': 'AGGREGATED_TOTAL',
            #              'timeDimension': {
            #                   'timeRange': {
            #                           'endTime': '2019-09-30T23:59:59.045123456Z',
            #                           'startTime': '2019-09-01T01:01:23.045123456Z'
            #                    }
            #               },
            #        'value': '223855'
            #        }
            #        },
            # metric_value.get('metric')
            # metric_value.get('value')
            # time.sleep(2.0)
            if metric_value.get('metric') == "QUERIES_DIRECT":
                # import ipdb; ipdb.set_trace()
                # QUERIES_DIRECT  The number of times the resource was shown when searching for the location directly.
                print(u'QUERIES_DIRECT | número de vezes que a localização foi mostrada quando pesquisada diretamente: ' + metric_value.get('totalValue').get('value'))
                worksheet_location_metric.write(location_metric_line, col, metric_value.get('totalValue').get('value'))
                col = col + 1
            elif metric_value.get('metric') == "QUERIES_INDIRECT":
                # QUERIES_INDIRECT    The number of times the resource was shown as a result of a categorical search (for example, restaurant).
                print(u'QUERIES_INDIRECT | O número de vezes que o recurso foi mostrado como resultado de uma pesquisa categórica (por exemplo, restaurante): ' + metric_value.get('totalValue').get('value'))
                worksheet_location_metric.write(location_metric_line, col, metric_value.get('totalValue').get('value'))
                col = col + 1
            elif metric_value.get('metric') == "QUERIES_CHAIN":
                # QUERIES_CHAIN   The number of times a resource was shown as a result of a search for the chain it belongs to, or a brand it sells. For example, Starbucks, Adidas. This is a subset of QUERIES_INDIRECT.
                print(u'QUERIES_CHAIN | O número de vezes que um recurso foi mostrado como resultado de uma pesquisa da cadeia a que pertence ou de uma marca que vende. Por exemplo, Starbucks, Adidas. Este é um subconjunto de QUERIES_INDIRECT: ' + metric_value.get('totalValue').get('value'))
                worksheet_location_metric.write(location_metric_line, col, metric_value.get('totalValue').get('value'))
                col = col + 1
            elif metric_value.get('metric') == "VIEWS_MAPS":
                # VIEWS_MAPS  The number of times the resource was viewed on Google Maps.
                print(u'VIEWS_MAPS | O número de vezes que o recurso foi visualizado no Google Maps: ' + metric_value.get('totalValue').get('value'))
                worksheet_location_metric.write(location_metric_line, col, metric_value.get('totalValue').get('value'))
                col = col + 1
            elif metric_value.get('metric') == "VIEWS_SEARCH":
                # VIEWS_SEARCH    The number of times the resource was viewed on Google Search.
                print(u'VIEWS_SEARCH | O número de vezes que o recurso foi visualizado na Pesquisa do Google: ' + metric_value.get('totalValue').get('value'))
                worksheet_location_metric.write(location_metric_line, col, metric_value.get('totalValue').get('value'))
                col = col + 1
            elif metric_value.get('metric') == "ACTIONS_WEBSITE":
                # ACTIONS_WEBSITE The number of times the website was clicked.
                print(u'ACTIONS_WEBSITE | : O número de vezes que o site foi clicado: ' + metric_value.get('totalValue').get('value'))
                worksheet_location_metric.write(location_metric_line, col, metric_value.get('totalValue').get('value'))
                col = col + 1
            elif metric_value.get('metric') == "ACTIONS_PHONE": 
                # ACTIONS_PHONE   The number of times the phone number was clicked.
                print(u'ACTIONS_PHONE | O número de vezes que o número de telefone foi clicado: ' + metric_value.get('totalValue').get('value'))
                worksheet_location_metric.write(location_metric_line, col, metric_value.get('totalValue').get('value'))
                col = col + 1
            elif metric_value.get('metric') == "ACTIONS_DRIVING_DIRECTIONS": 
                # ACTIONS_DRIVING_DIRECTIONS  The number of times driving directions were requested.
                print(u'ACTIONS_DRIVING_DIRECTIONS | O número de vezes que instruções de direção foram solicitadas: ' + metric_value.get('totalValue').get('value'))
                worksheet_location_metric.write(location_metric_line, col, metric_value.get('totalValue').get('value'))
                col = col + 1
            elif metric_value.get('metric') == "PHOTOS_VIEWS_MERCHANT": 
                # PHOTOS_VIEWS_MERCHANT   The number of views on media items uploaded by the merchant.
                print(u'PHOTOS_VIEWS_MERCHANT | O número de visualizações nos itens de mídia enviados pelo comerciante: ' + metric_value.get('totalValue').get('value'))
                worksheet_location_metric.write(location_metric_line, col, metric_value.get('totalValue').get('value'))
                col = col + 1
            elif metric_value.get('metric') == "PHOTOS_VIEWS_CUSTOMERS": 
                # PHOTOS_VIEWS_CUSTOMERS  The number of views on media items uploaded by customers.
                print(u'PHOTOS_VIEWS_CUSTOMERS | O número de visualizações nos itens de mídia carregados pelos clientes: ' + metric_value.get('totalValue').get('value'))
                worksheet_location_metric.write(location_metric_line, col, metric_value.get('totalValue').get('value'))
                col = col + 1
            elif metric_value.get('metric') == "PHOTOS_COUNT_MERCHANT": 
                # PHOTOS_COUNT_MERCHANT   The total number of media items that are currently live that have been uploaded by the merchant.
                print(u'PHOTOS_COUNT_MERCHANT | O número total de itens de mídia atualmente ativos que foram carregados pelo comerciante: ' + metric_value.get('totalValue').get('value'))
                worksheet_location_metric.write(location_metric_line, col, metric_value.get('totalValue').get('value'))
                col = col + 1
            elif metric_value.get('metric') == "PHOTOS_COUNT_CUSTOMERS": 
                # PHOTOS_COUNT_CUSTOMERS  The total number of media items that are currently live that have been uploaded by customers.
                print(u'PHOTOS_COUNT_CUSTOMERS | O número total de itens de mídia atualmente ativos que foram enviados por clientes: ' + metric_value.get('totalValue').get('value'))
                worksheet_location_metric.write(location_metric_line, col, metric_value.get('totalValue').get('value'))
                col = col + 1
            elif metric_value.get('metric') == "LOCAL_POST_VIEWS_SEARCH": 
                # LOCAL_POST_VIEWS_SEARCH The number of times the local post was viewed on Google Search.
                print(u'LOCAL_POST_VIEWS_SEARCH | O número de vezes que a postagem local foi visualizada na Pesquisa do Google: ' + metric_value.get('totalValue').get('value'))
                worksheet_location_metric.write(location_metric_line, col, metric_value.get('totalValue').get('value'))
                col = col + 1
            elif metric_value.get('metric') == "LOCAL_POST_ACTIONS_CALL_TO_ACTION": 
                # LOCAL_POST_ACTIONS_CALL_TO_ACTION   The number of times the call to action button was clicked on Google.
                print(u'LOCAL_POST_ACTIONS_CALL_TO_ACTION | O número de vezes que o botão de call to action foi clicado no Google: ' + metric_value.get('value'))
                worksheet_location_metric.write(location_metric_line, col, metric_value.get('totalValue').get('value'))
                col = col + 1

        location_metric_line = location_metric_line + 1

    #
    #
    #
    # drivingDirectionsRequest
    #
    #
    #
    body = {
        "locationNames": [
            location_name_address
        ],
        "drivingDirectionsRequest": { # A location indexed with the regions that people usually come from. This is captured by counting how many driving-direction requests to this location are from each region.
            "numDays": 90 # The number of days data is aggregated over.
            "regionCount": { # A region with its associated request count.
                "count": 50 # Number of driving-direction requests from this region.
            }
        }   
    }
    
    # url = 'https://mybusiness.googleapis.com/v4/{name=accounts/*}/locations:reportInsights')
    response = requests.post('https://mybusiness.googleapis.com/v4/'+account_name+'/locations:reportInsights', 
                        headers=headers, 
                        json=body) 
    
    location_driving_direction_metrics = json.loads(response.text)
    
    for location_driving_direction_metric in location_driving_direction_metrics.get("locationDrivingDirectionMetrics")[0].get("topDirectionSources")[0].get("regionCounts"):
        col = 0 
        worksheet_driving_request_metric.write(driving_metric_line, col, location_name_address)
        col = col + 1
        worksheet_driving_request_metric.write(driving_metric_line, col, location_driving_direction_metrics.get("locationDrivingDirectionMetrics")[0].get('topDirectionSources')[0].get('dayCount'))
        col = col + 1
        worksheet_driving_request_metric.write(driving_metric_line, col, location_driving_direction_metric.get('latlng').get('latitude'))
        col = col + 1
        worksheet_driving_request_metric.write(driving_metric_line, col, location_driving_direction_metric.get('latlng').get('longitude'))
        col = col + 1
        worksheet_driving_request_metric.write(driving_metric_line, col, location_driving_direction_metric.get('label'))
        col = col + 1
        worksheet_driving_request_metric.write(driving_metric_line, col, location_driving_direction_metric.get('count'))
        col = col + 1
    
        driving_metric_line = driving_metric_line + 1

    #
    #
    #
    # Reviews
    #
    #
    #
    # https://mybusiness.googleapis.com/v4/accounts/115754111690128178671/locations/12580825083561213126/reviews
    #
    # https://mybusiness.googleapis.com/v4/accounts/115754111690128178671/locations/12580825083561213126/reviews
    # ABHRLXW8Ex_WNzyu9nM2SrjrSfkpjMnXHkJ_wJ9DB2ybq46c6h40f14
    has_nextPageToken = True
    body = None
    while has_nextPageToken:

        if body:
            response = requests.get('https://mybusiness.googleapis.com/v4/'+location_name_address+'/reviews', headers=headers, params=body) 
        else:
            response = requests.get('https://mybusiness.googleapis.com/v4/'+location_name_address+'/reviews', headers=headers) 
    
        reviews_page = json.loads(response.text)
        pprint.pprint(reviews_page)

        if reviews_page.get("reviews"):
            for review in reviews_page.get("reviews"):

                # pprint.pprint(response.text, depth=1, width=60)
                # 'reviews': [
                #     {'createTime': '2019-02-27T14:33:23.629Z',
                #       'name': 'accounts/115754111690128178671/locations/12580825083561213126/reviews/AIe9_BEAgUIalCkcXZnurOUpP4uArLD-86kOM5nn-lRq9s0vun89XsFHWHi3c-2MVv1w1F4oQMZsltgoZLmAviDGztWr7pKoys2aeL5ldkoM88gj_5aIdy0',
                #       'reviewId': 'AIe9_BEAgUIalCkcXZnurOUpP4uArLD-86kOM5nn-lRq9s0vun89XsFHWHi3c-2MVv1w1F4oQMZsltgoZLmAviDGztWr7pKoys2aeL5ldkoM88gj_5aIdy0',
                #       'reviewer': {'displayName': 'Leonardo Ferrari',
                #                    'profilePhotoUrl': 'https://lh3.googleusercontent.com/-wFttlWlDouM/AAAAAAAAAAI/AAAAAAAAAAA/3HxlGL5_LV0/c-rp-mo-ba3-br100/photo.jpg'},
                #       'starRating': 'FIVE',
                #       'updateTime': '2019-02-27T14:33:23.629Z'},
                #      {'comment': 'Excelente lugar, é impossvel ir no  Bertioga, e '
                #                  'não gostar, tem opções para todas idades, está '
                #                  'aprovadíssimo ! !',
                #       'createTime': '2019-02-26T22:00:04.246Z',
                #       'name': 'accounts/115754111690128178671/locations/12580825083561213126/reviews/AIe9_BFmRFRFwJGRfUDOW8jG3rXnWMONjL-N1tICiZHRa0jtJwODRThPnU1kcmPZiC5iLTQnK9ESEl6cOygsgGfn_joQ1JJmIgcO3GkT7yn-zCiwwJawVXE',
                #       'reviewId': 'AIe9_BFmRFRFwJGRfUDOW8jG3rXnWMONjL-N1tICiZHRa0jtJwODRThPnU1kcmPZiC5iLTQnK9ESEl6cOygsgGfn_joQ1JJmIgcO3GkT7yn-zCiwwJawVXE',
                #       'reviewer': {'displayName': 'Jair Soares',
                #                    'profilePhotoUrl': 'https://lh5.googleusercontent.com/-2VV9sZnOZas/AAAAAAAAAAI/AAAAAAAAAAA/6ll2Guh9kaI/c-rp-mo-ba4-br100/photo.jpg'},
                #       'starRating': 'FIVE',
                #       'updateTime': '2019-02-26T22:00:04.246Z'},
                
                col = 0
                worksheet_reviews.write(review_line, col, location_name_address)
                col = col + 1
                worksheet_reviews.write(review_line, col, review.get('reviewer').get('displayName'))
                col = col + 1
                worksheet_reviews.write(review_line, col, review.get('reviewer').get('profilePhotoUrl'))
                col = col + 1
                worksheet_reviews.write(review_line, col, review.get('starRating'))
                col = col + 1
                if review.get('comment'):
                    worksheet_reviews.write(review_line, col, review.get('comment'))
                else:
                    worksheet_reviews.write(review_line, col, '')
                col = col + 1
                worksheet_reviews.write(review_line, col, review.get('updateTime'))
                col = col + 1
                worksheet_reviews.write(review_line, col, review.get('createTime'))
                col = col + 1
                review_line = review_line + 1

        if not reviews_page.get("nextPageToken"):
            has_nextPageToken = False
        else:
            body = {"pageToken": reviews_page.get("nextPageToken")}

    #
    #
    #
    # Questions
    #
    #
    #
    # https://mybusiness.googleapis.com/v4/accounts/115754111690128178671/locations/12580825083561213126/questions
    #
    # https://mybusiness.googleapis.com/v4/accounts/115754111690128178671/locations/12580825083561213126/questions
    # ABHRLXW8Ex_WNzyu9nM2SrjrSfkpjMnXHkJ_wJ9DB2ybq46c6h40f14
    has_nextPageToken = True
    body = None
    while has_nextPageToken:

        if body:
            response = requests.get('https://mybusiness.googleapis.com/v4/'+location_name_address+'/questions', headers=headers, params=body) 
        else:
            response = requests.get('https://mybusiness.googleapis.com/v4/'+location_name_address+'/questions', headers=headers) 
    
        questions_page = json.loads(response.text)
        pprint.pprint(questions_page)

        if questions_page.get("questions"):
            for question in questions_page.get("questions"):

                #
                # Questions
                #
                # "author": {
                #     "displayName": "Eduardo Henrique Pereira Pereira",
                #     "profilePhotoUrl": "//lh4.ggpht.com/-ox9XEh95V3o/AAAAAAAAAAI/AAAAAAAAAAA/hGV91x1ligs/c0x00000000-cc-rp-mo/photo.jpg",
                #     "type": "REGULAR_USER"
                #   },
                #   "createTime": "2019-10-20T13:18:32.211208Z",
                #   "name": "accounts/115754111690128178671/locations/12580825083561213126/questions/AIe9_BH-Y7Lwr6IxuH7aUb2G6RG2I7Zd58YBfNwtTqShf9-YUz_1SIBS3p5OOUOWr5L7c9TEfDcaHJ5NSeqL5Ox4R5ImezQf0vczVct5u4cvYRAEQt-22iWNAfGI--w6Sy1YCiwDANey",
                #   "text": "Bom dia,Como eu posso conhecer o   Bertioga ",

                col = 0
                worksheet_questions.write(question_line, col, location_name_address)
                col = col + 1
                worksheet_questions.write(question_line, col, question.get('name'))
                col = col + 1
                worksheet_questions.write(question_line, col, question.get('author').get('displayName'))
                col = col + 1
                worksheet_questions.write(question_line, col, question.get('author').get('profilePhotoUrl'))
                col = col + 1
                worksheet_questions.write(question_line, col, question.get('author').get('type'))
                col = col + 1
                worksheet_questions.write(question_line, col, question.get('text'))
                col = col + 1
                worksheet_questions.write(question_line, col, question.get('createTime'))
                col = col + 1
                question_line = question_line + 1
                
                if question.get('topAnswers'):
                    for answer in question.get('topAnswers'):

                        # "author": {
                        #     "displayName": "César Morais",
                        #     "profilePhotoUrl": "//lh3.ggpht.com/-RYMDqop1oN0/AAAAAAAAAAI/AAAAAAAAAAA/U1EQRLw-99A/c0x00000000-cc-rp-mo-ba3/photo.jpg",
                        #     "type": "LOCAL_GUIDE"
                        #   },
                        #   "createTime": "2019-10-20T14:47:15.273260Z",
                        #   "name": "accounts/115754111690128178671/locations/12580825083561213126/questions/AIe9_BH-Y7Lwr6IxuH7aUb2G6RG2I7Zd58YBfNwtTqShf9-YUz_1SIBS3p5OOUOWr5L7c9TEfDcaHJ5NSeqL5Ox4R5ImezQf0vczVct5u4cvYRAEQt-22iWNAfGI--w6Sy1YCiwDANey/answers/AIe9_BFu3rdicGrPrzdyu4PDXmqMuEM7Ezl_u8drbRWQo-LLUBAgghpt685nDfOxNa38mUhthKysTf5UDqT7lY6pK9TTRbukvFZL9uMpUzH4MpAlVIXoBhQOjZtYdS660i8U-XWl9yc6XGED-DWbw_2wigi_Lo99EP02rxuEzKeqNJZO7tW8ZYufrydFmMQ38uvEa3IwUO_-",
                        #   "text": "Você precisa ter a carteirinha do . Pode verificar pelo site centrodeferias.sp.org.br\nLá aparecem as opções para inscrição em viagens daqui a 6 meses (dependerá ainda de um sorteio) ou vagas de desistentes.\nOu então pode comparecer no  mais próximo e pegar informações de excursões para passar 1 dia no  Bertioga.\nEspero ter ajudado.",
                        #   "updateTime": "2019-10-20T14:47:15.273260Z"

                        col = 0
                        worksheet_answers.write(answers_line, col, location_name_address)
                        col = col + 1
                        worksheet_answers.write(answers_line, col, answer.get('name'))
                        col = col + 1
                        worksheet_answers.write(answers_line, col, answer.get('author').get('displayName'))
                        col = col + 1
                        worksheet_answers.write(answers_line, col, answer.get('author').get('profilePhotoUrl'))
                        col = col + 1
                        worksheet_answers.write(answers_line, col, answer.get('author').get('type'))
                        col = col + 1
                        worksheet_answers.write(answers_line, col, answer.get('text'))
                        col = col + 1
                        worksheet_answers.write(answers_line, col, answer.get('createTime'))
                        col = col + 1
                        worksheet_answers.write(answers_line, col, answer.get('updateTime'))
                        col = col + 1
                        answers_line = answers_line + 1
                
        if not questions_page.get("nextPageToken"):
            has_nextPageToken = False
        else:
            body = {"pageToken": questions_page.get("nextPageToken")}

workbook.close()