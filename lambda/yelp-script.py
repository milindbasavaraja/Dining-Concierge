import requests
from requests.structures import CaseInsensitiveDict
import json
from decimal import Decimal
url = "https://api.yelp.com/v3/businesses/search"

headers = CaseInsensitiveDict()
headers["Accept"] = "application/json"
headers["Authorization"] = "Bearer FGK-M0qUotM8Jemh6qpOnWYkVYiPYKG_q9CIKB4SoPuPseQRgYyNBAABZQ6TX1ZuHJfg0Yh4nYQtHdfFCU0u36ygKp-98iOg71a-LvinxbC2UqLc2rs7HbP5TXFRY3Yx"
locations_brooklyn = ['Sunset Park','Fort Greene','Williamsburg','Boerum Hill','Brighton Beach','Clinton Hill','Crown Heights','Gowanus','Park Slope','Dumbo','Cobble Hill','Bushwick','Greenpoint','Bedford-Stuyvesant','Coney Island','Downtown Brooklyn','Red Hook','Bay Ridge','Prospect Heights','Flatbush','Carroll Gardens']
locations_brooklyn
locations_manhattan = ['Lower East Side','East Harlem','Washington Heights','SoHo','East Village','Battery Park City','Chinatown','Lower Manhattan','Chelsea','Murray Hill','Upper West Side','Midtown','Governors Island','Times Square','Harlem','West Village','Roosevelt Island','Gramercy','Little Italy','Meatpacking District','Inwood','TriBeCa','Flatiron District','Hell\'s Kitchen','Central Park','Union Square','NoHo','Upper East Side','Greenwich Village','NoLIta','Koreatown']
locations = locations_brooklyn + locations_manhattan
len(locations)
for location in locations:
    parameters = {
      'location': location,
        'limit': '50',
        'offset': 950
    }
    response = requests.get(url, headers=headers,params=parameters)
    file_name = location+".json"
    f = open(file_name,"w")

    print(json.dump(response.json(),f))

import boto3
client = boto3.client('dynamodb',aws_access_key_id='AKIA6EYOWSUQVL6UHO5M', aws_secret_access_key='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', region_name='us-east-1')
dynamoDb = boto3.resource('dynamodb',aws_access_key_id='AKIA6EYOWSUQVL6UHO5M', aws_secret_access_key='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', region_name='us-east-1')
dynamoDb.Table('yelp-restaurants')
response.json()['businesses'][0]
data = []
location = 'Sunset Park'
term='restaurants'
for offset in range(0, 1000, 50):
    params = {
            'limit': 50, 
            'location': location.replace(' ', '+'),
            'term': term.replace(' ', '+'),
            'offset': offset
    }

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data += response.json()['businesses']
    elif response.status_code == 400:
        print('400 Bad Request')
        break
parameters = {
      'location':'Brooklyn' ,
        'limit':50,
        'offset':950
        
    }
response = requests.get(url, headers=headers,params=parameters)
#len(response.json()['businesses'])
count=0
for id in data:
    print(id['id'])
    count = count+1
    
print(count)
api ={}
single_json = json.loads(json.dumps(response.json()['businesses'][0]), parse_float=Decimal)
single_json
api[single_json['id']] = single_json
single_json
dynamoDb.Table('yelp-restaurants').put_item(Item=single_json)
#Scanning table


## HIT YELP API for all locations
data = []
location = 'Sunset Park'
term='restaurants'
for offset in range(0, 1000, 50):
    params = {
            'limit': 50, 
            'location': location.replace(' ', '+'),
            'term': term.replace(' ', '+'),
            'offset': offset
    }

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data += response.json()['businesses']
    elif response.status_code == 400:
        print('400 Bad Request')
        break
datas = data
datas

for data in datas:
    single_json_item = json.loads(json.dumps(data), parse_float=Decimal)
    api[single_json_item['id']] = single_json_item

api
for data in datas:
    print(data['id'])
    
data0 = datas[0]
data0['id']

from tqdm import tqdm
terms = ['chinese','french','german','indian','japanese','italian','mediterranean','mexican','american']
import datetime
datas = []
term ='restaurant'
for l in tqdm(range(len(locations))):
    location = locations[l]
        
    for offset in tqdm(range(0, 1000, 50)):
        params = {
                'limit': 50, 
                'location': location.replace(' ', '+'),
                'term': term.replace(' ', '+'),
                'offset': offset
        }        
        
        response = requests.get(url, headers=headers, params=params)
       
        if response.status_code == 200:
            datas += response.json()['businesses']   
    print("The data size is:",len(datas))
def clean(data):
    data['id'] = data['id']
    data['name'] = data['name']
    data['display_address'] = data['display_address']
    data['coordinates'] = data['coordinates']
    data['review_count'] = data['review_count']
    data['rating'] = data['rating']
    data['categories'] = data['categories']
    data['is_closed'] = data['is_closed']
    data['display_phone'] = data['display_phone']
    del data['url']
    del data['alias']
    del data['image_url']
    del data['transactions']
    del data['price']
    del data['location']
    del data['phone']
    del data['distance']
    
    return data    
api
len(datas)
api ={}
data = {}
for d in range(len(datas)):

    data['id'] = datas[d]['id']
    data['name'] = datas[d]['name']
    data['latitude'] = datas[d]['coordinates']['latitude']
    data['longitude'] = datas[d]['coordinates']['longitude']
    data['review_count'] = datas[d]['review_count']
    data['rating'] = datas[d]['rating']
    data['categories'] = datas[d]['categories']
    data['is_closed'] = datas[d]['is_closed']
    data['display_phone'] = datas[d]['display_phone']
    data['location'] = datas[d]['location']['address1']
    data['zipcode'] = datas[d]['location']['zip_code']
    current_time = datetime.datetime.now()
    time = current_time.strftime("%m/%d/%Y %H:%M:%S")
    data['time'] = time
            
    
    single_json_item = json.loads(json.dumps(data), parse_float=Decimal)
    api[single_json_item['id']] = single_json_item
len(api)

for key in api:
    print(key)

count = 1
for key in api:
    print("Adding record "+ str(count)+ " to database")
    dynamoDb.Table('yelp-restaurants').put_item(Item=api[key])
    count = count + 1
response = dynamoDb.Table('yelp-restaurants').scan()
response
response = dynamoDb.Table('yelp-restaurants').scan()
count = 5
es = {}
for i in response['Items']:
    es['id'] = i['id']
    es['categories'] = i['categories']
    print(es)
    count = count - 1
    if count==0:
        break
   

file_name = "data.json"
f = open(file_name,"w")

print(json.dump(api,f))
host = "https://search-restaurant-w6ibzg2wc7ngye2w5f7kw35odm.us-east-1.es.amazonaws.com"

region = "us-east-1"
service = "es"
response = dynamoDb.Table('yelp-restaurants').scan()
response

es = {}
count = 0

for item in response['Items']:
    es['id'] = item['id']
    es['categories'] = item['categories']
    payload = es 
    count = count + 1
    path = "/restaurants/_doc/"+str(count)+"/"
    url = host + path
    response = requests.post(url,auth=('milind','Milind@123'),json=payload)
    print("Number of elements pushed to elastic is: ",count)
        
        
print("Done")
