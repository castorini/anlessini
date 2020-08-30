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

    print('Starting at {}'.format(datetime.datetime.fromtimestamp(time.time())))
    with open(args.collection) as f:
        for i, line in enumerate(f):
            id, doc = line.rstrip().split('\t')

            msmarco.put_item(
                Item = {
                    'id': int(id),
                    'content': doc
                })

            if i % 100 == 0:
                print('Inserted id {}...'.format(i))
    print('Ending at {}'.format(datetime.datetime.fromtimestamp(time.time())))
