package io.anlessini.store;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

import java.io.InputStream;
import java.io.IOException;
import java.util.*;


public class S3Directory extends Directory {
  private static final Logger LOG = LogManager.getLogger(S3Directory.class);

  private final AmazonS3 s3Client;
  private final Map<String, Integer> lengths = new HashMap<>();

  private String bucket;
  private String key;

  private int cacheThreshold = 1024*1024*512;

  public S3Directory(String bucket, String key) {
    this.bucket = bucket;
    this.key = key;

    s3Client = AmazonS3ClientBuilder.standard()
        .withRegion(Regions.US_EAST_1)
        .build();
  }

  public int getCacheThreshold() {
    return cacheThreshold;
  }

  public void setCacheThreshold(int threshold) {
    this.cacheThreshold = threshold;
  }

  @Override
  public String[] listAll() throws IOException {
    // https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/java/example_code/s3/src/main/java/ListKeys.java
    ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket).withMaxKeys(1024);
    ListObjectsV2Result result = s3Client.listObjectsV2(req);

    List<String> list = new ArrayList<>();
    for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
      list.add(objectSummary.getKey().split("/")[1]);
      lengths.put(objectSummary.getKey().split("/")[1], (int) objectSummary.getSize());
    }
    return list.toArray(new String[list.size()]);
  }

  @Override
  public void deleteFile(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long fileLength(String name) throws IOException {
    throw new UnsupportedOperationException();
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
  public IndexOutput createTempOutput(String s, String s1, IOContext ioContext) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sync(Collection<String> collection) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void syncMetaData() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rename(String s, String s1) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    if (lengths.get(name) < cacheThreshold) {
      LOG.info("[openInput] " + name + ", size = " + lengths.get(name) + " - caching!");
      String fullName = key + "/" + name;
      S3Object object = s3Client.getObject(new GetObjectRequest(bucket, fullName));
      // hack around lack of readNBytes in Java 8
      byte[] data = readNBytes(object.getObjectContent(), Integer.MAX_VALUE);

      object.close();

      if (data.length != lengths.get(name)) {
        throw new IOException();
      }

      return new S3IndexInput(name, data);
    }

    LOG.info("[openInput] " + name + ", size = " + lengths.get(name) + " - not caching!");
    return new S3IndexInput(name);
  }

  @Override
  public Lock obtainLock(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
  }

  private class S3IndexInput extends IndexInput {
    private int pos = 0;
    private String name;
    private String fullName;
    private byte[] data = null;

    protected S3IndexInput(String name) {
      super(name);
      this.name = name;
      this.fullName = key + "/" + name;
      LOG.trace("creating S3IndexInput on " + this.fullName);
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
      return lengths.get(name);
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

  // This is a hack around the fact that Java 8 is the only runtime available for AWS Java Lambdas.
  // We get a NoSuchMethodError exception on com.amazonaws.services.s3.model.S3ObjectInputStream.readAllBytes()
  // because InputStream only added readAllBytes and readNBytes in JDK 9.
  // Thus, I copied this implementation:
  // https://github.com/openjdk/jdk11u/blob/master/src/java.base/share/classes/java/io/InputStream.java

  private static final int DEFAULT_BUFFER_SIZE = 8192;
  private static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;

  public static byte[] readNBytes(InputStream in, int len) throws IOException {
    if (len < 0) {
      throw new IllegalArgumentException("len < 0");
    }

    List<byte[]> bufs = null;
    byte[] result = null;
    int total = 0;
    int remaining = len;
    int n;
    do {
      byte[] buf = new byte[Math.min(remaining, DEFAULT_BUFFER_SIZE)];
      int nread = 0;

      // read to EOF which may read more or less than buffer size
      while ((n = in.read(buf, nread,
          Math.min(buf.length - nread, remaining))) > 0) {
        nread += n;
        remaining -= n;
      }

      if (nread > 0) {
        if (MAX_BUFFER_SIZE - total < nread) {
          throw new OutOfMemoryError("Required array size too large");
        }
        total += nread;
        if (result == null) {
          result = buf;
        } else {
          if (bufs == null) {
            bufs = new ArrayList<>();
            bufs.add(result);
          }
          bufs.add(buf);
        }
      }
      // if the last call to read returned -1 or the number of bytes
      // requested have been read then break
    } while (n >= 0 && remaining > 0);

    if (bufs == null) {
      if (result == null) {
        return new byte[0];
      }
      return result.length == total ?
          result : Arrays.copyOf(result, total);
    }

    result = new byte[total];
    int offset = 0;
    remaining = total;
    for (byte[] b : bufs) {
      int count = Math.min(b.length, remaining);
      System.arraycopy(b, 0, result, offset, count);
      offset += count;
      remaining -= count;
    }

    return result;
  }
}
