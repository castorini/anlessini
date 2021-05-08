# anlessini

## Requirements

- [Anserini](https://github.com/castorini/anserini): an open-source information retrieval toolkit built on Lucene.
- Java 11+
- Python 3.7+
- AWS CLI
- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)

## Get Started
   
First let's build the project.

```bash
$ mvn clean install
```

Anlessini uses AWS SAM/Cloudformation for describing the infrastructure.
So let's create a S3 bucket for storing the artifacts.

```bash
$ ./bin/create-artifact-bucket.sh
```

Now let's provision the AWS infrastructure for Anlessini.
We recommend that you spin up individual CloudFormation stack for each of the collection, as they are logically isolated.
The following is an example of Anlessini serving [COVID-19 Open Research Dataset](https://github.com/castorini/anserini/blob/master/docs/experiments-cord19.md).

```bash
# package the artifact and upload to S3
$ sam package --template-file template.yaml --s3-bucket $(cat artifact-bucket.txt) --output-template-file cloudformation/cord19.yaml --s3-prefix cord19
# create cloudformation stack
$ sam deploy --template-file cloudformation/cord19.yaml $(cat artifact-bucket.txt) --s3-prefix cord19 --stack-name cord19 --capabilities CAPABILITY_NAMED_IAM
```

Now we have our infrastructure up, we can populate S3 with our index files, and import the corpus into DynamoDB.

We will be using [Anserini](https://github.com/castorini/anserini) to index our corpus, so please refer to the [documentation](https://github.com/castorini/anserini/tree/master/docs) for your specific corpus. 

First, download and extract the corpus.

```bash
$ cd /path/to/anserini
$ curl https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/historical_releases/cord-19_2020-10-09.tar.gz -o collections/cord19-2020-10-09.tar.gz
$ pushd collections && tar -zxvf cord19-2020-10-09.tar.gz && rm cord19-2020-10-09.tar.gz && popd
```

Now we will build the Lucene index.
Note that we **do not enable** `-storeContents`, `-storeRaw`, or `-storePositions` to keep the index minimal. 
Keeping an index small helps speed up search queries.

```bash
$ cd /path/to/anserini
$ mvn clean package appassembler:assemble
$ target/appassembler/bin/IndexCollection \
    -collection Cord19AbstractCollection -generator Cord19Generator \
    -threads 8 -input collections/cord19-2020-10-09 \
    -index indexes/lucene-index-cord19-abstract-2020-10-09 \
    -storeDocvectors
```

Now lets upload the index files to S3.

```bash
$ cd /path/to/anserini
$ export INDEX_BUCKET=$(aws cloudformation describe-stacks --stack-name cord19 --query "Stacks[0].Outputs[?OutputKey=='IndexBucketName'].OutputValue" --output text)
$ aws s3 cp indexes/lucene-index-cord19-abstract-2020-10-09/ s3://$INDEX_BUCKET/cord19/ --recursive
```

To import the corpus into DynamoDB, use the `ImportCollection` util.
You may first run the command with `-dryrun` option to perform validation and sanity check without writing to DynamoDB. 
If everything goes well in the dryrun, you can write the document contents to DynamoDB.

```bash
$ cd /path/to/anlessini
$ export DYNAMO_TABLE=$(aws cloudformation describe-stacks --stack-name cord19 --query "Stacks[0].Outputs[?OutputKey=='DynamoTableName'].OutputValue" --output text)
$ utils/target/appassembler/bin/ImportCollection \
    -collection Cord19AbstractCollection -generator Cord19Generator \
    -dynamo.table $DYNAMO_TABLE \
    -threads 8 -input /path/to/anserini/collections/cord19-2020-10-09
```

Now we can try invoking our function:

```bash
$ export API_URL=$(aws cloudformation describe-stacks --stack-name cord19 --query "Stacks[0].Outputs[?OutputKey=='SearchApiUrl'].OutputValue" --output text)
$ curl $API_URL\?query\=incubation\&max_docs\=3
```
