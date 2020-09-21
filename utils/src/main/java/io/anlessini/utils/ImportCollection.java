package io.anlessini.utils;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.*;
import io.anserini.collection.DocumentCollection;
import io.anserini.collection.FileSegment;
import io.anserini.collection.SourceDocument;
import io.anserini.index.IndexArgs;
import io.anserini.index.generator.*;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.kohsuke.args4j.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ImportCollection {
  private static final Logger LOG = LogManager.getLogger(ImportCollection.class);

  public static class ImportArgs {
    @Option(name = "-input", metaVar = "[path]", required = true,
        usage = "Location of input collection.")
    public String input;

    @Option(name = "-threads", metaVar = "[num]", required = true,
        usage = "Number of indexing threads.")
    public int threads = 8;

    @Option(name = "-collection", metaVar = "[class]", required = true,
        usage = "Collection class in package 'io.anserini.collection'.")
    public String collectionClass;

    @Option(name = "-generator", metaVar = "[class]",
        usage = "Document generator class in package 'io.anserini.index.generator'.")
    public String generatorClass = "DefaultLuceneDocumentGenerator";

    @Option(name = "-dynamo.table", required = true,
        usage = "The DynamoDB table name to import the collection to.")
    public String dynamoTable;

    @Option(name = "-dynamo.poolSize", metaVar = "[num]",
        usage = "DynamoDB client pool size.")
    public int dynamoPoolSize = 8;

    @Option(name = "-dynamo.batchSize", metaVar = "[num]",
        usage = "Batch size of the BatchWriteItem operation, capped at 25")
    public int dynamoBatchSize = 25;

    @Option(name = "-dynamo.provisioned", metaVar = "",
        usage = "Set DynamoDB table's billing mode to provisioned, which is by default on-demand.")
    public boolean dynamoProvisioned;

    @Option(name = "-dynamo.capacity.read", metaVar = "[num]",
        usage = "Set DynamoDB table's provisioned read capacity.")
    public long dynamoReadCapacityUnit = 5;

    @Option(name = "-dynamo.capacity.write", metaVar = "[num]",
        usage = "Set DynamoDB table's provisioned write capacity.")
    public long dynamoWriteCapacityUnit = 5;
  }

  private final ImportArgs args;
  private final Path collectionPath;
  private final Class collectionClass;
  private final Class generatorClass;
  private final Counters counters;
  private final DocumentCollection collection;
  private ObjectPool<AmazonDynamoDB> dynamoPool;

  public final class Counters {
    /**
     * Counter for successfully imported documents
     */
    public AtomicLong imported = new AtomicLong();

    /**
     * Counter for empty documents that are not imported
     */
    public AtomicLong empty = new AtomicLong();

    /**
     * Counter for unindexable documents
     */
    public AtomicLong unindexable = new AtomicLong();

    /**
     * Counter for skipped documents. These are cases documents are skipped as part of normal
     * processing logic, e.g., using a whitelist, not indexing retweets or deleted tweets.
     */
    public AtomicLong skipped = new AtomicLong();

    /**
     * Counter for unexpected errors
     */
    public AtomicLong errors = new AtomicLong();
  }

  private class DynamoClientFactory extends BasePooledObjectFactory<AmazonDynamoDB> {
    @Override
    public AmazonDynamoDB create() {
      return AmazonDynamoDBClientBuilder.defaultClient();
    }

    @Override
    public PooledObject<AmazonDynamoDB> wrap(AmazonDynamoDB dynamoDB) {
      return new DefaultPooledObject<>(dynamoDB);
    }

    @Override
    public void destroyObject(PooledObject<AmazonDynamoDB> pooled) {
      pooled.getObject().shutdown();
    }
  }

  public ImportCollection(ImportArgs args) throws Exception {
    this.args = args;

    collectionPath = Paths.get(args.input);
    if (!Files.exists(collectionPath) || !Files.isReadable(collectionPath) || !Files.isDirectory(collectionPath)) {
      throw new RuntimeException("Document directory " + collectionPath.toString() + " does not exist or is not readable, please check the path");
    }

    generatorClass = Class.forName("io.anserini.index.generator." + args.generatorClass);
    collectionClass = Class.forName("io.anserini.collection." + args.collectionClass);

    collection = (DocumentCollection) collectionClass.getConstructor(Path.class).newInstance(collectionPath);

    GenericObjectPoolConfig<AmazonDynamoDB> config = new GenericObjectPoolConfig<>();
    config.setMaxTotal(args.dynamoPoolSize);
    config.setMinIdle(args.dynamoPoolSize);
    dynamoPool = new GenericObjectPool<>(new DynamoClientFactory(), config);

    counters = new Counters();
  }

  public void run() {
    Configurator.setRootLevel(Level.INFO);
    final long start = System.nanoTime();
    LOG.info("============ Import Collection ============");

    final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(args.threads);
    LOG.info("Thread pool with " + args.threads + " threads initialized.");

    LOG.info("Initializing collection in " + collectionPath.toString());
    final List segmentPaths = collection.getSegmentPaths();
    final int segmentCnt = segmentPaths.size();
    LOG.info(String.format("%,d %s found", segmentCnt, (segmentCnt == 1 ? "file" : "files" )));

//    LOG.info("Creating DynamoDB table " + args.dynamoTable);
//    AmazonDynamoDB dynamoDB = null;
//    try {
//      dynamoDB = dynamoPool.borrowObject();
//      CreateTableRequest createTableRequest = new CreateTableRequest()
//          .withTableName(args.dynamoTable)
//          .withKeySchema(new KeySchemaElement().withKeyType(KeyType.HASH).withAttributeName(IndexArgs.ID))
//          .withAttributeDefinitions(new AttributeDefinition(IndexArgs.ID, ScalarAttributeType.S));
//      if (args.dynamoProvisioned) {
//        createTableRequest = createTableRequest
//            .withBillingMode(BillingMode.PROVISIONED)
//            .withProvisionedThroughput(new ProvisionedThroughput(args.dynamoReadCapacityUnit, args.dynamoWriteCapacityUnit));
//      } else {
//        createTableRequest = createTableRequest
//            .withBillingMode(BillingMode.PAY_PER_REQUEST);
//      }
//      CreateTableResult createTableResult = dynamoDB.createTable(createTableRequest);
//      LOG.info("Created DynamoDB table " + args.dynamoTable + ", table description: ");
//      LOG.info(createTableResult.getTableDescription().toString());
//    } catch (ResourceInUseException e) {
//      throw new RuntimeException("Error occurred when creating table " + args.dynamoTable + ". ImportCollection can " +
//          "only import data into new DynamoDB tables, please avoid using existing table names.", e);
//    } catch (Exception e) {
//      throw new RuntimeException("Error occurred when creating table " + args.dynamoTable, e);
//    } finally {
//      try {
//        dynamoPool.returnObject(dynamoDB);
//      } catch (Exception e) {
//        LOG.error("Error returning DynamoDB client to pool", e);
//      }
//    }

    LOG.info("Starting to import...");
    for (int i = 0; i < segmentCnt; i++) {
      executor.execute(new ImporterThread((Path) segmentPaths.get(i), collection));
    }

    executor.shutdown();

    try {
      // Wait for existing tasks to terminate
      while (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
        if (segmentCnt == 1) {
          LOG.info(String.format("%,d documents imported", counters.imported.get()));
        } else {
          LOG.info(String.format("%.2f%% of files completed, %,d documents imported",
              (double) executor.getCompletedTaskCount() / segmentCnt * 100.0d, counters.imported.get()));
        }
      }
    } catch (InterruptedException ie) {
      // (Re-)Cancel if current thread also interrupted
      executor.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }

    LOG.info("Import Complete!");
    LOG.info("============ Final Counter Values ============");
    LOG.info(String.format("imported:    %,12d", counters.imported.get()));
    LOG.info(String.format("unindexable: %,12d", counters.unindexable.get()));
    LOG.info(String.format("empty:       %,12d", counters.empty.get()));
    LOG.info(String.format("skipped:     %,12d", counters.skipped.get()));
    LOG.info(String.format("errors:      %,12d", counters.errors.get()));

    final long durationMillis = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    LOG.info(String.format("Total %,d documents indexed in %s", counters.imported.get(),
        DurationFormatUtils.formatDuration(durationMillis, "HH:mm:ss")));
  }

  private final class ImporterThread extends Thread {
    private static final int MAX_REQUEST_RETRIES = 3;
    private final Path input;
    private final DocumentCollection collection;
    private FileSegment<SourceDocument> fileSegment;
    private List<Document> batch;

    private ImporterThread(Path input, DocumentCollection collection) {
      this.input = input;
      this.collection = collection;
      setName(input.getFileName().toString());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
      try {
        LuceneDocumentGenerator generator =
            (LuceneDocumentGenerator) generatorClass.getDeclaredConstructor(IndexArgs.class).newInstance(new IndexArgs());

        long cnt = 0;
        batch = new ArrayList<>();

        // in order to call close() and clean up resources in case of exception
        fileSegment = collection.createFileSegment(input);
        for (SourceDocument d: fileSegment) {
          if (!d.indexable()) {
            counters.unindexable.incrementAndGet();
            continue;
          }

          Document doc;
          try {
            doc = generator.createDocument(d);
          } catch (GeneratorException e) {
            if (e instanceof EmptyDocumentException) {
              counters.empty.incrementAndGet();
            } else if (e instanceof SkippedDocumentException) {
              counters.skipped.incrementAndGet();
            } else if (e instanceof InvalidDocumentException) {
              counters.errors.incrementAndGet();
            } else {
              LOG.error("Unhandled exception in document generation", e);
            }
            continue;
          }

          batch.add(doc);
          if (batch.size() >= args.dynamoBatchSize) {
            sendBatchRequest();
            cnt += batch.size();
            counters.imported.addAndGet(batch.size());
            batch = new ArrayList<>();
          }
        }

        if (batch.size() > 0) {
          sendBatchRequest();
          cnt += batch.size();
          counters.imported.addAndGet(batch.size());
        }

        int skipped = fileSegment.getSkippedCount();
        if (skipped > 0) {
          counters.skipped.addAndGet(skipped);
          LOG.warn(input.getParent().getFileName().toString() + File.separator +
              input.getFileName().toString() + ": " + skipped + " docs skipped.");
        }

        if (fileSegment.getErrorStatus()) {
          counters.errors.incrementAndGet();
          LOG.error(input.getParent().getFileName().toString() + File.separator +
              input.getFileName().toString() + ": error iterating through segment.");
        }

        LOG.debug(input.getParent().getFileName().toString() + File.separator +
            input.getFileName().toString() + ": " + cnt + " docs added.");
      } catch (Exception e) {
        LOG.error(Thread.currentThread().getName() + ": Unexpected exception:", e);
      } finally {
        if (fileSegment != null) {
          fileSegment.close();
        }
      }
    }

    private void sendBatchRequest() throws Exception {
      int retries = 0;
      TableWriteItems items = new TableWriteItems(args.dynamoTable)
          .withItemsToPut(batch.stream().map(this::toDynamoDBItem).collect(Collectors.toList()));
      LOG.info("Writing " + items.getItemsToPut().size() + " items to DynamoDB");

      AmazonDynamoDB dynamoDBClient = dynamoPool.borrowObject();
      DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
      try {
        BatchWriteItemOutcome outcome = null;
        while (outcome == null) {
          try {
            outcome = dynamoDB.batchWriteItem(items);
          } catch (ProvisionedThroughputExceededException e) {
            LOG.error("Error sending BatchWriteItem request to DynamoDB", e);
            if (retries++ < MAX_REQUEST_RETRIES) {
              Thread.sleep((long) (Math.pow(2, retries) * 1000));
            } else {
              throw new RuntimeException("BatchWriteItem failed after too many retries", e);
            }
          } catch (ResourceNotFoundException | RequestLimitExceededException |
              ItemCollectionSizeLimitExceededException | InternalServerErrorException e) {
            LOG.error("Encountered non-retriable error", e);
            throw new RuntimeException("BatchWriteItem failed due to non-retriable error", e);
          }
        }

        Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();
        retries = 0;
        BatchWriteItemResult writeUnprocessedResult;
        while (!unprocessedItems.isEmpty()) {
          try {
            writeUnprocessedResult = dynamoDBClient.batchWriteItem(unprocessedItems);
            unprocessedItems = writeUnprocessedResult.getUnprocessedItems();
          } catch (ProvisionedThroughputExceededException e) {
            LOG.error("Error sending BatchWriteItem request to DynamoDB", e);
            if (retries++ < MAX_REQUEST_RETRIES) {
              Thread.sleep((long) (Math.pow(2, retries) * 1000));
            } else {
              throw new RuntimeException("BatchWriteItem failed after too many retries", e);
            }
          } catch (ResourceNotFoundException | RequestLimitExceededException |
              ItemCollectionSizeLimitExceededException | InternalServerErrorException e) {
            LOG.error("Encountered non-retriable error", e);
            throw new RuntimeException("BatchWriteItem failed due to non-retriable error", e);
          }
        }
      } finally {
        dynamoPool.returnObject(dynamoDBClient);
      }
    }

    public Item toDynamoDBItem(Document doc) {
      Item ret = new Item();
      Map<String, List<IndexableField>> documentFields = new HashMap<>();
      for (IndexableField field: doc.getFields()) {
        List<IndexableField> fields = documentFields.getOrDefault(field.name(), new LinkedList<>());
        fields.add(field);
        documentFields.put(field.name(), fields);
      }

      for (Map.Entry<String, List<IndexableField>> entry: documentFields.entrySet()) {
        String fieldName = entry.getKey();
        List<String> fieldValues = entry.getValue().stream()
            .map(IndexableField::stringValue)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        ret.with(fieldName, fieldValues);
      }

      // override single-value fields
      ret.with(IndexArgs.ID, doc.getField(IndexArgs.ID).stringValue());
      ret.with(IndexArgs.CONTENTS, doc.getField(IndexArgs.CONTENTS).stringValue());
      if (doc.getField(IndexArgs.RAW) != null) {
        ret.with(IndexArgs.RAW, doc.getField(IndexArgs.RAW).stringValue());
      }

      return ret;
    }
  }

  public static void main(String[] args) throws Exception {
    ImportArgs importCollectionArgs = new ImportArgs();
    CmdLineParser parser = new CmdLineParser(importCollectionArgs, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.err.println("Example: " + ImportCollection.class.getSimpleName() +
          parser.printExample(OptionHandlerFilter.REQUIRED));
      System.exit(1);
    }

    new ImportCollection(importCollectionArgs).run();
  }
}
