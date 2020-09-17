package io.anlessini.store;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class S3Directory extends BaseDirectory {
  private static final Logger LOG = LogManager.getLogger(S3Directory.class);
  private static final int DEFAULT_CACHE_THRESHOLD = 1024*1024*512; // 512 MB

  private final AmazonS3 s3Client;

  private Map<String, S3ObjectSummary> objectSummaries;

  private final String bucket;
  private final String key;
  private final int cacheThreshold;

  private final Lock lsLock = new ReentrantLock();

  public S3Directory(String bucket, String key) {
    this(bucket, key, DEFAULT_CACHE_THRESHOLD);
  }

  public S3Directory(String bucket, String key, int cacheThreshold) {
    super(new SingleInstanceLockFactory());
    this.bucket = bucket;
    this.key = key;
    this.cacheThreshold = cacheThreshold;

    s3Client = AmazonS3ClientBuilder.defaultClient();
    LOG.info("Opened S3Directory under " + bucket + "/" + key);
  }

  @Override
  public String[] listAll() {
    lsLock.lock();
    if (objectSummaries == null) { // only ls if has not already done so, otherwise use cached result
      objectSummaries = new HashMap<>();
      String listingCursor = null;
      ListObjectsV2Result result;
      do {
        ListObjectsV2Request req = new ListObjectsV2Request()
            .withBucketName(bucket)
            .withPrefix(key + "/")
            .withStartAfter(listingCursor);
        result = s3Client.listObjectsV2(req);
        List<S3ObjectSummary> listings = result.getObjectSummaries();
        for (S3ObjectSummary objectSummary: listings) {
          String objectKey = objectSummary.getKey();
          String objectName = objectKey.split("/")[1];
          objectSummaries.put(objectName, objectSummary);
        }
        listingCursor = listings.get(listings.size() - 1).getKey();
      } while (result.isTruncated());
    }
    lsLock.unlock();

    String[] result = objectSummaries.keySet().toArray(new String[objectSummaries.size()]);
    Arrays.sort(result);
    return result;
  }

  @Override
  public void deleteFile(String s) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long fileLength(String name) {
    return objectSummaries.get(name).getSize();
  }

  @Override
  public Set<String> getPendingDeletions() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexOutput createOutput(String s, IOContext ioContext) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexOutput createTempOutput(String s, String s1, IOContext ioContext) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sync(Collection<String> collection) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void syncMetaData() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rename(String s, String s1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    if (fileLength(name) < cacheThreshold) {
      LOG.info("[openInput] " + name + ", size = " + fileLength(name) + " - caching!");
      String fullName = key + "/" + name;
      S3Object object = s3Client.getObject(new GetObjectRequest(bucket, fullName));
      byte[] data = object.getObjectContent().readAllBytes();

      object.close();

      if (data.length != fileLength(name)) {
        throw new IOException();
      }

      return new S3IndexInput(name, data);
    }

    LOG.info("[openInput] " + name + ", size = " + fileLength(name) + " - not caching!");
    return new S3IndexInput(name);
  }

  @Override
  public void close() {
    s3Client.shutdown();
  }

  private class S3IndexInput extends IndexInput {
    private int pos = 0;
    private String name;
    private String fullName;
    private byte[] data;

    protected S3IndexInput(String name) {
      this(name, null);
    }

    protected S3IndexInput(String name, byte[] data) {
      super(name);
      this.name = name;
      this.fullName = key + "/" + name;
      this.data = data;
      LOG.trace("creating S3IndexInput on " + this.fullName);
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public long getFilePointer() {
      return pos;
    }

    @Override
    public void seek(long pos) throws IOException {
      LOG.trace("[seek]      " + bucket + "/" + fullName + " to position " + pos);
      this.pos = (int) pos;
    }

    @Override
    public long length() {
      return fileLength(name);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      // Note that this only works if we've cached the entire file.
      LOG.trace("[slice]     " + bucket + "/" + fullName + " " +
          sliceDescription + ", at position " + pos + ", offset " + offset + ", length " + length);
      S3IndexInput slice = new S3IndexInput(name, Arrays.copyOfRange(data, (int) offset, (int) (offset + length)));
      return slice;
    }

    @Override
    public byte readByte() throws IOException {
      LOG.trace("[readByte]  " + bucket + "/" + fullName + " at position " + pos);
      if (data != null) {
        byte b = data[pos];
        pos++;
        return b;
      }

      GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucket, fullName).withRange(pos, pos);
      S3Object objectPortion = s3Client.getObject(rangeObjectRequest);
      pos++;
      byte b = (byte) objectPortion.getObjectContent().read();

      objectPortion.close();
      return b;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      LOG.trace("[readBytes] " + bucket + "/" + fullName + " at position " + pos + ", offset " + offset + ", length " + len);
      if (len == 0)
        return;

      if (data != null) {
        // Read into the offset
        System.arraycopy(data, pos, b, offset, len);
        pos += len;
        return;
      }

      GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucket, fullName).withRange(pos, pos+len-1);
      S3Object objectPortion = s3Client.getObject(rangeObjectRequest);
      byte[] buf = objectPortion.getObjectContent().readAllBytes();
      objectPortion.close();
      System.arraycopy(buf, 0, b, offset, len);
      pos += len;
    }
  }
}
