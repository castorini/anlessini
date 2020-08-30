import argparse
import boto3
import json

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, help='id', required=True)

    args = parser.parse_args()

    dynamodb = boto3.resource('dynamodb',region_name='us-east-1')
    msmarco = dynamodb.Table('msmarco')

    response = msmarco.get_item(Key={'id': args.id})

    print(response['Item']['content'])
