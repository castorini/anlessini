package io.anlessini.store;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class S3CachedReader {
  private static final Logger LOG = LogManager.getLogger(S3CachedReader.class);
  static final int DEFAULT_BLOCK_SIZE = 1024 * 1024 * 64; // 64 MB
  static final int DEFAULT_INITIAL_CACHE_SIZE = 16;
  static final float DEFAULT_LOAD_FACTOR = 0.75f;
  static final int DEFAULT_CONCURRENCY_LEVEL = 16;
  static final long MAX_HEAP_SIZE = 1024 * 1024 * 1536; // 1536 MB

  private final AmazonS3 s3Client;
  private final Map<CacheKey, List<CacheEntry>> cache;
  /**
   * Cache size in bytes
   */
  private final AtomicLong size;

  public S3CachedReader(AmazonS3 s3Client) {
    this.s3Client = s3Client;
    this.size = new AtomicLong(0L);
    this.cache = new ConcurrentHashMap<>(DEFAULT_INITIAL_CACHE_SIZE, DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL);
  }

  protected int read(ByteBuffer b, S3ObjectSummary objectSummary, long offset, int length) throws IOException {
    return readFromS3(b, objectSummary, offset, length);
  }

  protected int readFromS3(ByteBuffer b, S3ObjectSummary objectSummary, long offset, int length) throws IOException {
    LOG.info("[readingFromS3][" + toString() + "@" + hashCode() + "] offset=" + offset + " length=" + length);
    GetObjectRequest rangeObjectRequest = new GetObjectRequest(objectSummary.getBucketName(), objectSummary.getKey())
        .withRange(offset, offset + length - 1);
    S3Object object = s3Client.getObject(rangeObjectRequest);
    ReadableByteChannel objectContentChannel = Channels.newChannel(object.getObjectContent());
    int bytesRead = IOUtils.read(objectContentChannel, b);
    object.close();
    return bytesRead;
  }

  public static class CacheEntry {
    public final long offset;
    public final byte[] data;

    public CacheEntry(long offset, byte[] data) {
      this.offset = offset;
      this.data = data;
    }
  }

  public static class CacheKey {
    public final String bucket;
    public final String key;

    public CacheKey(String bucket, String key) {
      this.bucket = bucket;
      this.key = key;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CacheKey that = (CacheKey) o;

      if (!bucket.equals(that.bucket)) return false;
      return key.equals(that.key);
    }

    @Override
    public int hashCode() {
      int result = bucket.hashCode();
      result = 31 * result + key.hashCode();
      return result;
    }
  }
}
