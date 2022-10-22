import json
import boto3
import uuid
import logging

log = logging.getLogger()
log.setLevel(logging.DEBUG)
logging.basicConfig(format = '%(asctime)s : %(levelname)s %(message)s')

def lambda_handler(event, context):
    logging.debug("The event is: %s",event)
    message = event['messages'][0]['unstructured']['text']
    lex = boto3.client('lex-runtime')
    lex_response = lex.post_text(
        botName = 'DiningChat',
        botAlias = 'dev',
        userId = 'user',
        inputText = message
    )
    
    log.debug("The response is: %s",lex_response)
    
    response = {
        "messages":[
            {
                "type":"unstructured",
                "unstructured":{
                    "text" : lex_response['message']
                }
            }
            ]
    }
    
    return response