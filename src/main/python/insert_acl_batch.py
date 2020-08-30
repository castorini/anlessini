import boto3
from pyserini.search import SimpleSearcher


client = boto3.resource("dynamodb")
table = client.Table("ACL")

searcher = SimpleSearcher("../../../acl")

def get_multivalued_field(doc, field):
    return [field.stringValue() for field in doc.getFields(field)]

keys = {
    "id": "single",
    "title": "single",
    "abstract_html": "single",
    "authors": "multi",
    "year": "single",
    "url": "single",
    "venues": "multi",
    "sigs": "multi"
}

total_paper_inserted = 0
queue = []

for i in range(searcher.num_docs):
    paper = searcher.doc(i).lucene_document()
    cur_input = {}

    # seeing if we need to write to dynamodb
    if total_paper_inserted % 25 == 0 and total_paper_inserted != 0:
        client.batch_write_item(RequestItems={'ACL': queue})
        queue = []
        print("Done inserting", total_paper_inserted, "papers")

    # inserting content that can be searched
    search_content = searcher.doc(i).contents()

    # getting the fields
    for key in keys:
        cur_input[key] = get_multivalued_field(paper, key) if keys[key] == "multi" else paper.get(key)
        # checking if it is a null value
        cur_input[key] = cur_input[key] if cur_input[key] else "None"
    cur_input["searchable"] = search_content 

    # appending to be bulk inserted
    queue.append({'PutRequest': {'Item': cur_input}})
    total_paper_inserted += 1

