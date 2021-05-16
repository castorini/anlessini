import torch
from transformers import pipeline
import json 

def lambda_handler(event, context):
    QnA_pipeline = pipeline('question-answering', model='.')
    #body = json.loads(event)
    result = QnA_pipeline({
        'context': event['queryStringParameters']['context'],
        'question': event['queryStringParameters']['question']
    })
    return {
        "statusCode": 200,
        "body": json.dumps({'result': result}),
        "headers": {
            'Content-Type': 'text/html',
        }
    }