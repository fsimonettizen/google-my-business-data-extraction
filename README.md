# google-my-business-data-extraction

### install project

`pip install -r requirements.txt` to run locally

### 

1) deploy cloud formation (aws/deploy.yml)
2) Generate client_secret_json from google api to translate text from portugues (pt_br) to english (sentiment analysis can only be executed in english)
3) fill postgres config file (postgres_config.py)
4) don't forget it to upload the following libraries into the source.zip file
	* urllib3
	* rsa
	* requests
	* pyasn1_modules
	* pyasn1
	* psycopg2
	* oauth2client
	* idna
	* httplib2
	* chardet
	* certifi

### To build project to AWS

1) ./build_lambda_function.sh 

### Table prefixes 

+ gmb_*               Google My Business
+ custom_words*       Processed by internal words custom scripts
+ aws_comprehend_*    Processed by AWS Comprehend API (sentiment, key_phrase and entities)
...

### Table descriptions

#### gmb_location

Google My business location identity. 

### gmb_location_metrics

Google My business location metrics separated by start_time and name (gmb_location foreign key)

#### attributes*

+ QUERIES_DIRECT  The number of times the resource was shown when searching for the location directly.
+ QUERIES_INDIRECT    The number of times the resource was shown as a result of a categorical search (for example, restaurant).
+ QUERIES_CHAIN   The number of times a resource was shown as a result of a search for the chain it belongs to, or a brand it sells. For example, Starbucks, Adidas. This is a subset of QUERIES_INDIRECT.
+ VIEWS_MAPS  The number of times the resource was viewed on Google Maps.
+ VIEWS_SEARCH    The number of times the resource was viewed on Google Search.
+ ACTIONS_WEBSITE The number of times the website was clicked.
+ ACTIONS_PHONE   The number of times the phone number was clicked.
+ ACTIONS_DRIVING_DIRECTIONS  The number of times driving directions were requested.
+ PHOTOS_VIEWS_MERCHANT   The number of views on media items uploaded by the merchant.
+ PHOTOS_VIEWS_CUSTOMERS  The number of views on media items uploaded by customers.
+ PHOTOS_COUNT_MERCHANT   The total number of media items that are currently live that have been uploaded by the merchant.
+ PHOTOS_COUNT_CUSTOMERS  The total number of media items that are currently live that have been uploaded by customers.
+ LOCAL_POST_VIEWS_SEARCH The number of times the local post was viewed on Google Search.
+ LOCAL_POST_ACTIONS_CALL_TO_ACTION   The number of times the call to action button was clicked on Google.

### gmb_driving_request_metrics

Data from request directions on the google maps

### gmb_reviews, questions and answers 

All data from reviews, questions and answers from a location

### aws_compreend_sentiment, aws_compreend_key_phrase, aws_compreend_entity 
Data processed with AWS Comprehend from text_en fields on (reviews, questions and answers) 
