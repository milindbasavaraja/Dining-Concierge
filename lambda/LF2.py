import json
import logging
import boto3;
from datetime import datetime
import requests
import random

log = logging.getLogger()
log.setLevel(logging.DEBUG)
logging.basicConfig(format = '%(asctime)s : %(levelname)s %(message)s')

def retrieve_restaurants_elastic(cuisine):
    log.debug("Finding restaurants for cuisine: "+cuisine)
    region = "us-east-1"
    service = "es"
    
    domain_url = 'https://search-restaurant-w6ibzg2wc7ngye2w5f7kw35odm.us-east-1.es.amazonaws.com'
    index = '/restaurants'
    url = domain_url + index + '/_search'
    query = {
        "size": 25,
        "query": {
            "multi_match": {
                    "query": cuisine,
                    "fields": ["categories.title"]
            }
         }
    }
    
    headers = { "Content-Type": "application/json" }
    elastic_response = requests.get(url, auth=("milind","Milind@123"), headers=headers, data=json.dumps(query))
    log.debug("The response is"+elastic_response.text)
    response = {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": '*'
        },
        "isBase64Encoded": False
    }
    
    response['body'] = elastic_response.text
    response['body'] = json.loads(response['body'])
    #print(response['body'])
    restaurants_list = response['body']['hits']['hits']
    restaurantIdList = random.choices(restaurants_list, k=4)
    restaurantIdList = [restaurant['_source']['id'] for restaurant in random.choices(restaurants_list, k=5)]
    log.debug(restaurantIdList)
    return restaurantIdList
    
def retrieve_restaurant_details_dynamoDB(restaurants_id_list):
    log.debug("Retriving restaurant details from DynamoDB")
    dynamoDb = boto3.client("dynamodb")
    restaurant_details_list = []
    for id in restaurants_id_list:
        dynamo_response = dynamoDb.get_item(
            TableName="yelp-restaurants",
            Key={
                "id": {
                    "S": id
                }
            }
        )
        restaurant_details_list.append(dynamo_response['Item'])
    log.debug(restaurant_details_list)
    return restaurant_details_list
    

def send_mail_to_user(restaurant_detail_set, email_addr,cuisine,location):
    ses = boto3.client('ses')
    verified_email_ids = ses.list_verified_email_addresses()
    if email_addr not in verified_email_ids['VerifiedEmailAddresses']:
        verify_email_response = ses.verify_email_identity(EmailAddress=email_addr)
        return
    message = "Hi, Here is the list of top 5 {} restaurants at {} I found that might suit you: ".format(cuisine, location)
    message_restaurant = ""
    count = 1
    for restaurant in restaurant_detail_set:
        restaurantName = restaurant['name']['S']
        restaurantAddress = restaurant['location']['S']
        restaurantZip = restaurant['zipcode']['S']
        reviewCount = restaurant['review_count']['N']
        ratings = restaurant['rating']['N']
        message_restaurant += str(count)+". {} located at {}, {} with Ratings of {} and {} reviews. ".format(restaurantName, restaurantAddress, restaurantZip, ratings, reviewCount)
        message_restaurant += "\n"
        count += 1
    log.debug(message_restaurant)
    # Send a mail to the user regarding the restaurant suggestions
    mailResponse = ses.send_email(
        Source="milindaws2@gmail.com",
        Destination={'ToAddresses': [email_addr]},
        Message={
            'Subject': {
                'Data': "Dining Conceirge Chatbot has a message for you!",
                'Charset': 'UTF-8'
            },
            'Body': {
                'Text': {
                    'Data': message+message_restaurant,
                    'Charset': 'UTF-8'
                },
                'Html': {
                    'Data': message+message_restaurant,
                    'Charset': 'UTF-8'
                }
            }
        }
    )

def lambda_handler(event, context):
    log.debug(event)
    
    log.debug("Polling SQS queue")
    sqs = boto3.client('sqs')
    queue_url = "https://sqs.us-east-1.amazonaws.com/972304061729/DiningInfoQueue"
    
    
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        WaitTimeSeconds=20
    )
    

    log.debug(response)
    log.debug("The length is")
    log.debug(len(response))
    
    if len(response)==1:
        log.debug("No Msg found")
        return ""
    receipt_handle = response['Messages'][0]['ReceiptHandle']
    message_attributes = response['Messages'][0]['MessageAttributes']
    log.debug("Deleting the message with ReceiptHandle: ")
    log.debug(receipt_handle)
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )
    
    contact_num = message_attributes['contactNum']['StringValue']
    email_id = message_attributes['EmailId']['StringValue']
    dining_date = message_attributes['DiningDate']['StringValue']
    cuisine = message_attributes['Cuisine']['StringValue']
    num_people = message_attributes['NumPeople']['StringValue']
    dining_time = message_attributes['DiningTime']['StringValue']
    location = message_attributes['Location']['StringValue']
    
    
    log.debug(contact_num)
    log.debug(email_id)
    log.debug(dining_date)
    log.debug(cuisine)
    log.debug(num_people)
    log.debug(dining_time)
    log.debug(location)
    log.debug(contact_num)

    restaurants_id_list = retrieve_restaurants_elastic(cuisine)
    restaurant_details = retrieve_restaurant_details_dynamoDB(restaurants_id_list)
    send_mail_to_user(restaurant_details,email_id,cuisine,location)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
