import argparse
import boto3
import json
import time
import datetime

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--collection', required=True, help='MS MARCO .tsv collection file')
    args = parser.parse_args()

    dynamodb = boto3.resource('dynamodb',region_name='us-east-1')
    msmarco = dynamodb.Table('msmarco')

    records = 0
    batch_writes = 0
    queue = []
    print('Starting at {}'.format(datetime.datetime.fromtimestamp(time.time())))
    with open(args.collection) as f:
        for i, line in enumerate(f):
            id, doc = line.rstrip().split('\t')

            queue.append({'PutRequest': {'Item': {'id': int(id), 'content': doc}}})
            records += 1

            if len(queue) == 25:
                dynamodb.batch_write_item(RequestItems={'msmarco': queue})
                batch_writes += 1
                queue = []

            if i > 0 and i % 100 == 0:
                print('id {}, {} batch_write_item calls'.format(i, batch_writes))

    dynamodb.batch_write_item(RequestItems={'msmarco': queue})
    print('{} total records written'.format(records))
    print('Ending at {}'.format(datetime.datetime.fromtimestamp(time.time())))
