package io.anlessini;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.anlessini.store.S3Directory;
import org.apache.commons.logging.Log;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;

import java.io.*;
import java.util.Map;

import org.json.JSONObject;

public class SearchLambda {
  private static final String INDEX_BUCKET = "INDEX_BUCKET";
  private static final String INDEX_KEY = "INDEX_KEY";

  private static S3Directory directory = null;
  private static IndexReader reader = null;

  private String bucket;
  private String key;

  public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context)
      throws IOException {

    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    StringBuilder sb = new StringBuilder();
    String line;
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }
    JSONObject json = new JSONObject(sb.toString());


    ObjectMapper mapper = new ObjectMapper();
    LambdaLogger logger = context.getLogger();

    logger.log("Input: \n");
    String qstring = json.getJSONObject("queryStringParameters").get("query").toString();

    String hits = search(qstring, context);
    outputStream.write(("{\"statusCode\": 200, \"headers\": { \"Access-Control-Allow-Origin\": \"*\" }," +
        "\"body\": " + mapper.writeValueAsString(hits) + "}").getBytes());
  }

  public String search(String qstring, Context context) throws IOException {
    LambdaLogger logger = context.getLogger();
    logger.log("received : " + qstring + "\n");

    long startTime = System.currentTimeMillis();

    // initializing the dynamodb instance
    AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    DynamoDB dynamoDB = new DynamoDB(client);
    Table table = dynamoDB.getTable("ACL");

    if (directory == null) {
      bucket = System.getenv(INDEX_BUCKET);
      key = System.getenv(INDEX_KEY);
      logger.log("Initializing index: " + bucket + "/" + key + "\n");
      directory = new S3Directory(bucket, key);
      directory.setCacheThreshold(1024 * 1024 * 512);
      reader = DirectoryReader.open(directory);
    }

    Analyzer analyzer = new EnglishAnalyzer();
    Similarity similarity = new BM25Similarity(0.9f, 0.4f);
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setSimilarity(similarity);
    searcher.setQueryCache(null);     // disable query caching

    logger.log("Query: " + qstring + "\n");
    Query query = SearchDemo.buildQuery("contents", analyzer, qstring);
    TopDocs topDocs = searcher.search(query, 10);

    logger.log("Number of hits: " + topDocs.scoreDocs.length + "\n");

    String[] docids = new String[topDocs.scoreDocs.length];
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      Document doc;
      doc = reader.document(topDocs.scoreDocs[i].doc);
      String docid = doc.getField("id").stringValue();
      docids[i] = docid;
    }

    ObjectMapper mapper = new ObjectMapper();
    ArrayNode rootNode = mapper.createArrayNode();
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      logger.log(docids[i] + " " + topDocs.scoreDocs[i].score + "\n");
      ObjectNode childNode = mapper.createObjectNode();
      childNode.put("docid", docids[i]);
      childNode.put("score", topDocs.scoreDocs[i].score);

      // retreiving from dynamodb
      Item item = table.getItem("id", docids[i]);
      childNode.put("json_result", item.toJSONPretty());
      rootNode.add(childNode);
    }
    long endTime = System.currentTimeMillis();
    logger.log("Query latency: " + (endTime - startTime) + " ms" + "\n");

    return mapper.writeValueAsString(rootNode);
  }
}
