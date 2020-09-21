package io.anlessini;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.anlessini.store.S3Directory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class SearchLambda implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
  private static final Logger LOG = LogManager.getLogger(SearchLambda.class);

  private final S3Directory directory;
  private final IndexReader reader;
  private static final String S3_INDEX_BUCKET = System.getenv("INDEX_BUCKET");
  private static final String S3_INDEX_KEY = System.getenv("INDEX_KEY");

  private final DynamoDB dynamoDB;
  private final String DYNAMODB_TABLE_NAME = System.getenv("TABLE_NAME");
  private final Table dynamoTable;

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final ObjectWriter writer = objectMapper.writer();

  public SearchLambda() throws IOException {
    directory = new S3Directory(S3_INDEX_BUCKET, S3_INDEX_KEY);
    reader = DirectoryReader.open(directory);

    // initializing the dynamodb instance
    dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder.defaultClient());
    dynamoTable = dynamoDB.getTable(DYNAMODB_TABLE_NAME);
  }

  @Override
  public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent input, Context context) {
    APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
    String queryString = input.getQueryStringParameters().get("query");

    try {
      String hits = search(queryString);
      response.setStatusCode(200);
      response.setBody(hits);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      response.setStatusCode(500);
      try {
        response.setBody(writer.writeValueAsString(Map.of("message", e.getMessage())));
      } catch (JsonProcessingException ex) {
        LOG.error("Error writing JSON message", ex);
      }
    }

    response.setHeaders(Map.of(
        "Content-Type", "application/json"
    ));
    return response;
  }

  public String search(String qstring) throws IOException {
    long startTime = System.currentTimeMillis();
    Analyzer analyzer = new EnglishAnalyzer();
    Similarity similarity = new BM25Similarity(0.9f, 0.4f);
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setSimilarity(similarity);
    searcher.setQueryCache(null);     // disable query caching

    LOG.info("Query: " + qstring);
    Query query = SearchDemo.buildQuery("contents", analyzer, qstring);
    TopDocs topDocs = searcher.search(query, 10);

    LOG.info("Number of hits: " + topDocs.scoreDocs.length);

    String[] docids = new String[topDocs.scoreDocs.length];
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      Document doc;
      doc = reader.document(topDocs.scoreDocs[i].doc);
      String docid = doc.getField("id").stringValue();
      docids[i] = docid;
    }

    ObjectNode rootNode = objectMapper.createObjectNode();
    rootNode.put("query_id", UUID.randomUUID().toString());
    ArrayNode response = objectMapper.createArrayNode();

    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      // retreiving from dynamodb
      Item item = dynamoTable.getItem("id", docids[i]);
      response.add(item.toJSON());
    }
    rootNode.set("response", response);
    // System.out.println(mapper.writeValueAsString(rootNode));

    long endTime = System.currentTimeMillis();
    LOG.info("Query latency: " + (endTime - startTime) + " ms");
    return objectMapper.writeValueAsString(rootNode);
  }
}
