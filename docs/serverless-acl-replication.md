## Deploying a serverless endpoint for querying ACL papers

This document describes the steps to deploy a serverless search service 
for the ACL Anthology dataset.

### Requirements
[Anserini](https://github.com/castorini/anserini) and [Anlessini](https://github.com/castorini/anlessini)

### Fetching ACL Anthology Data
Navigate to any directory outside of `Anserini` and `Anlessini`,
clone the ACL anthology repository containing the raw XML data.
```bash
git clone git@github.com:acl-org/acl-anthology.git
```
Next navigate to the `acl-anthology` folder and install dependencies:
```bash
pip install -r bin/requirements.txt
```
You may have to create the export directory by hand (this is generated 
by the Makefile automatically, so if you don't run `make` you'll need 
this extra step):
```bash
mkdir -p build/data
```
Generate cleaned YAML data:
```bash
python bin/create_hugo_yaml.py
```
Generated ACL files can now be found in `acl-anthology/build/data/`

### Indexing Data
Now navigate to the `anserini` root folder and run the following commands:
```bash
cd collections && mkdir acl-data
cd ..
mvn clean package
```
Now move all the files within `acl-anthology/build/data/` into the newly created
`acl-data/`

Start generating lucene indexes by running:
```
sh target/appassembler/bin/IndexCollection \
  -collection AclAnthology -generator AclAnthologyGenerator \
  -threads 8 -input collections/acl-data \
  -index indexes/acl \
  -storePositions -storeDocvectors -storeContents -storeRaw
```
The generated lucene indexes will be stored in the `indexes/acl/` directory.

### Populating AWS services
Create a new S3 bucket called `acl-indexes`, and within it create a folder called
`acl`, navigate to the folder and upload the files in `indexes/acl/` into it.

