import json
import logging;
import boto3;
import datetime

log = logging.getLogger()
log.setLevel(logging.DEBUG)
logging.basicConfig(format = '%(asctime)s : %(levelname)s %(message)s')

#Constant Fields
DIALOG_CODE_HOOK_INVOCATION_SOURCE = 'DialogCodeHook'

def get_slots(intent):
    return intent['currentIntent']['slots']

def validate_slots(slots):
    if not slots['Location']:
        log.debug("Location slot not initialized")
        return {
            'isValid':False,
            'slotName': 'Location'
        }
        
    if not slots['Cuisine']:
        log.debug("Cuisine slot not initialized")
        return {
            'isValid':False,
            'slotName': 'Cuisine'
        }
    
    if not slots['DiningDate']:
        log.debug("Dining Date not initialized")
        return{
            'isValid':False,
            'slotName':'DiningDate'
        }
    else:
        date = slots['DiningDate']
        log.debug("The date is")
        log.debug(date)
        if datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
            log.debug("Wrong date")
            return {
                'isValid':False,
                'slotName':'DiningDate',
                'message' : "Sorry wrong date inserted, Please enter date again (Future dates only)"
            }
    
    if not slots['DiningTime']:
        log.debug("Dining Time not initialized")
        return{
            'isValid':False,
            'slotName':'DiningTime'
        }

    if not slots['NumPeople']:
        log.debug("NumPeople not initialized")
        return{
            'isValid':False,
            'slotName':'NumPeople'
        }
        
    if not slots['contactNum']:
        log.debug("contactNum not initialized")
        return{
            'isValid':False,
            'slotName':'contactNum'
        }
        
    if not slots['EmailId']:
        log.debug("EmailId not initialized")
        return{
            'isValid':False,
            'slotName':'EmailId'
        }
        
        
    
    return {
        'isValid': True
    }

def greeting_event(intent):
    log.debug("Greeting Intent")
    
    text = "Hi there, how can I help you?"
    intent_type = "ElicitIntent"
    
    response = {
        "dialogAction":{
            "type":intent_type,
            "message" :{
                "contentType" : "PlainText",
                "content": text
            }
        }
    }
    
    return response
    
def dining_intent(intent):
    log.debug("Dining Intent")
    log.debug("The intent request for dining is: %s",intent)
    
    intent_name = intent['currentIntent']['name']
    invocation_source = intent['invocationSource']
    slots = get_slots(intent)
    slot_validation = validate_slots(slots)
    intent_type = 'ElicitSlot'
    
    if invocation_source == DIALOG_CODE_HOOK_INVOCATION_SOURCE:
        if not slot_validation['isValid']:
            log.debug("%s slot is empty",slot_validation['slotName'])
            if 'message' in slot_validation:
                log.debug("Sending message in dialogAction")
                return {
                        "dialogAction": {
                            "type": intent_type,
                            "intentName": "DiningIntent",
                             "slots": slots,
                            "slotToElicit": slot_validation['slotName'],
                            "message" :{
                                    "contentType" : "PlainText",
                                    "content": slot_validation['message']
                                }
                       
                            }
                        }
            response = {
                        "dialogAction": {
                            "type": intent_type,
                            "intentName": "DiningIntent",
                             "slots": slots,
                            "slotToElicit": slot_validation['slotName']
                            }
                        }
            
            log.debug("The slot empty response is %s",response)
            return response
            
        else:
            logging.debug("All slots are filled: %s",slots)
            logging.debug("Sending message to queue")
            
            location    = slots['Location']
            cuisine     = slots['Cuisine']
            dining_time = slots['DiningTime']
            dining_date = slots['DiningDate']
            email       = slots['EmailId']
            num_people  = str(slots['NumPeople'])
            contact_num = str(slots['contactNum'])
            
            message = {
                
                'Cuisine' : {
                    'DataType' : 'String',
                    'StringValue' : cuisine
                },
                'Location' : {
                    'DataType' : 'String',
                    'StringValue' : location
                },
                'DiningTime': {
                    'DataType' : 'String',
                    'StringValue' : dining_time
                },
                'DiningDate' : {
                    'DataType' : 'String',
                    'StringValue' : dining_date
                },
                'EmailId' : {
                    'DataType' : 'String',
                    'StringValue' : email
                },
                'NumPeople' : {
                    'DataType' : 'String',
                    'StringValue' : num_people
                },
                'contactNum' : {
                    'DataType' : 'String',
                    'StringValue' : contact_num
                }
                
                
            }
            
            logging.debug("The type of message is %s",type(message))
            
            sqs = boto3.client('sqs')
            sqs.send_message(QueueUrl = 'https://sqs.us-east-1.amazonaws.com/972304061729/DiningInfoQueue',MessageAttributes = message, MessageBody = "Sending message from lam")
            response =  {
                            "sessionAttributes":intent['sessionAttributes'],
                            "dialogAction": {
                                "type": "Close",
                                "fulfillmentState": "Fulfilled",
                                "message" :{
                                    "contentType" : "PlainText",
                                    "content": "We have received your request. Expect the suggestion over mail in short time!"
                                }
                            }
                        }
            log.debug("The slots response is: %s",response)
            return response
            


def thanks_intent(intent):
    log.debug("Executing ThanksIntent")
    response =  {
    
            "dialogAction": {
                "type": "Close",
                "fulfillmentState": "Fulfilled",
                "message" :{
                    "contentType" : "PlainText",
                    "content": "Thank You! Please come again"
                }
            }
        }
    log.debug("The slots response is: %s",response)
    return response
    
    
def handle_intents(intent):
    intent_name = intent['currentIntent']['name']
    
    if intent_name == 'GreetingIntentq':
        return greeting_event(intent)
    elif intent_name == 'DiningIntent':
        return dining_intent(intent)
    elif intent_name == 'ThanksIntent':
        return thanks_intent(intent)
        
    
    log.error("Intent not configured. Please check Intent")
    raise Exception("Intent not configured.")

def lambda_handler(event, context):
    log.info("The incoming request from LEX is %s",event)
 
    
    slots = event['currentIntent']['slots']
    invocation_source = event['invocationSource']
    
    log.debug("The invocation source is:%s",invocation_source)
    return handle_intents(event)
  