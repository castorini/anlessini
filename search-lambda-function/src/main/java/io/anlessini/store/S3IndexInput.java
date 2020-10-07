package io.anlessini.store;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.BufferedIndexInput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class S3IndexInput extends BufferedIndexInput {
  private static final Logger LOG = LogManager.getLogger(S3IndexInput.class);
  private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024 * 4; // 4 MB

  private final AmazonS3 s3Client;
  private final S3CachedReader reader;
  private final S3ObjectSummary objectSummary;
  private ByteBuffer bytebuf;

  /**
   * The start offset in the entire file, non-zero in the slice case
   */
  private final long off;
  /**
   * The end offset
   */
  private final long end;

  public S3IndexInput(AmazonS3 s3Client, S3ObjectSummary objectSummary) {
    this(s3Client, objectSummary, 0, objectSummary.getSize(), defaultBufferSize(objectSummary.getSize()));
  }

  public S3IndexInput(AmazonS3 s3Client, S3ObjectSummary objectSummary, long offset, long length, int bufferSize) {
    super(objectSummary.getBucketName() + "/" + objectSummary.getKey(), bufferSize);
    this.s3Client = s3Client;
    this.reader = new S3CachedReader(s3Client);
    this.objectSummary = objectSummary;
    this.off = offset;
    this.end = offset + length;
    LOG.trace("Opened S3IndexInput " + toString() + "@" + hashCode() + " , bufferSize=" + getBufferSize());
  }

  private static int defaultBufferSize(long fileLength) {
    long bufferSize = fileLength;
    bufferSize = Math.max(bufferSize, MIN_BUFFER_SIZE);
    bufferSize = Math.min(bufferSize, DEFAULT_BUFFER_SIZE);
    return Math.toIntExact(bufferSize);
  }

  @Override
  public void close() throws IOException {
    // no-op
  }

  @Override
  public long length() {
    return end - off;
  }

  @Override
  protected void newBuffer(byte[] newBuffer) {
    super.newBuffer(newBuffer);
    bytebuf = ByteBuffer.wrap(newBuffer);
  }

  @Override
  public S3IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    if (offset < 0 || length < 0 || offset + length > this.length()) {
      throw new IllegalArgumentException("Slice " + sliceDescription + " out of bounds: " +
          "offset=" + offset + ",length=" + length + ",fileLength=" + this.length() + ": " + toString());
    }
    LOG.trace("[slice][" + toString() + "@" + hashCode() + "] " + getFullSliceDescription(sliceDescription) + ", offset=" + offset + ", length=" + length + ", fileLength=" + this.length());
    return new S3IndexInput(s3Client, objectSummary, off + offset, length, defaultBufferSize(length));
  }

  @Override
  public S3IndexInput clone() {
    S3IndexInput clone = (S3IndexInput) super.clone();
    LOG.trace("[clone][" + toString() + "@" + hashCode() + "], clone=" + clone.hashCode());
    return clone;
  }

  @Override
  protected void readInternal(byte[] b, int offset, int length) throws IOException {
    final ByteBuffer bb;

    if (b == buffer) { // using internal
      assert bytebuf != null;
      bytebuf.clear().position(offset);
      bb = bytebuf;
    } else {
      bb = ByteBuffer.wrap(b, offset, length);
    }

    synchronized (this) {
      long pos = getFilePointer() + this.off;

      if (pos + length > end) {
        throw new EOFException("Reading past EOF: " + toString() + "@" + hashCode());
      }

      try {
        int readLength = length;
        while (readLength > 0) {
          final int toRead = Math.min(DEFAULT_BUFFER_SIZE, readLength);
          bb.limit(bb.position() + toRead);
          assert bb.remaining() == toRead;
          final int bytesRead = reader.read(objectSummary, bb, pos, toRead);
          if (bytesRead < 0) {
            throw new EOFException("Read past EOF: " + toString() + "@" + hashCode() + " off=" + offset + " len=" + length + " pos=" + pos + " chunk=" + toRead + " end=" + end);
          }
          assert bytesRead > 0 : "Read with non zero-length bb.remaining() must always read at least one byte";
          pos += bytesRead;
          readLength -= bytesRead;
        }
        assert readLength == 0;
      } catch (IOException ioe) {
        throw new IOException(ioe.getMessage() + ": " + toString() + "@" + hashCode(), ioe);
      }
    }
  }

  @Override
  protected void seekInternal(long pos) throws IOException {
    if (pos > length()) {
      throw new EOFException("read past EOF: pos=" + pos + ", length=" + length() + ": " + toString() + "@" + hashCode());
    }
  }
}
