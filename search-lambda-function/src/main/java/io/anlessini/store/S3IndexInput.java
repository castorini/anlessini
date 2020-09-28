package io.anlessini.store;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

public class S3IndexInput extends BufferedIndexInput {
  private static final Logger LOG = LogManager.getLogger(S3IndexInput.class);
  private static final int CHUNK_SIZE = 1024*1024*64; // 64 MB

  private final AmazonS3 s3Client;
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

  public S3IndexInput(AmazonS3 s3Client, S3ObjectSummary objectSummary, IOContext context) {
    this(s3Client, objectSummary, context, 0, objectSummary.getSize());
  }

  public S3IndexInput(AmazonS3 s3Client, S3ObjectSummary objectSummary, IOContext context, long off, long length) {
    this(objectSummary.getBucketName() + "/" + objectSummary.getKey() + "@" + off + ":" + length,
        s3Client, objectSummary, context, off, length);
  }

  public S3IndexInput(String sliceDescription, AmazonS3 s3Client, S3ObjectSummary objectSummary, IOContext context, long off, long length) {
    this(sliceDescription, s3Client, objectSummary, off, length, bufferSize(context));
  }

  public S3IndexInput(String sliceDescription, AmazonS3 s3Client, S3ObjectSummary objectSummary, long off, long length, int bufferSize) {
    super(sliceDescription, bufferSize);
    this.s3Client = s3Client;
    this.objectSummary = objectSummary;
    this.off = off;
    this.end = off + length;
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
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    if (offset < 0 || length < 0 || offset + length > this.length()) {
      throw new IllegalArgumentException("slice()" + sliceDescription + " out of bounds: offset=" + offset
          + ", length=" + length + ", fileLength=" + this.length() + ": " + this);
    }
    LOG.trace(String.format("[slice][%s] %s/%s at offset %d, length %d",
        sliceDescription, objectSummary.getBucketName(), objectSummary.getKey(), offset, length));
    return new S3IndexInput(sliceDescription, s3Client, objectSummary, offset, length, getBufferSize());
  }

  @Override
  protected void newBuffer(byte[] newBuffer) {
    super.newBuffer(newBuffer);
    bytebuf = ByteBuffer.wrap(newBuffer);
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
        throw new EOFException("Reading past EOF: " + this);
      }

      try {
        int readLength = length;
        while (readLength > 0) {
          final int toRead = Math.min(CHUNK_SIZE, readLength);
          bb.limit(bb.position() + toRead);
          assert bb.remaining() == toRead;
          final int bytesRead = readFromS3(bb, pos, toRead);
          if (bytesRead < 0) {
            throw new EOFException("Read past EOF: " + this + " off=" + offset + " len=" + length + " pos=" + pos + " chunk=" + toRead + " end=" + end);
          }
          assert bytesRead > 0 : "Read with non zero-length bb.remaining() must always read at least one byte";
          pos += bytesRead;
          readLength -= bytesRead;
        }
        assert readLength == 0;
      } catch (IOException ioe) {
        throw new IOException(ioe.getMessage() + ": " + this, ioe);
      }
    }
  }

  protected int readFromS3(ByteBuffer b, long offset, int length) throws IOException {
    GetObjectRequest rangeObjectRequest = new GetObjectRequest(objectSummary.getBucketName(), objectSummary.getKey())
        .withRange(offset, offset + length - 1);
    S3Object object = s3Client.getObject(rangeObjectRequest);
    ReadableByteChannel objectContentChannel = Channels.newChannel(object.getObjectContent());
    int bytesRead = IOUtils.read(objectContentChannel, b);
    object.close();
    return bytesRead;
  }

  @Override
  protected void seekInternal(long pos) throws IOException {
    if (pos > length()) {
      throw new EOFException("read past EOF: pos=" + pos + ", length=" + length() + ": " + this);
    }
  }
}
