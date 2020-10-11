package io.anlessini.store;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.*;

public class S3CachedReader {
  private static final Logger LOG = LogManager.getLogger(S3CachedReader.class);

  /**
   * Try to read eagerly from S3 to reduce the number of total GETs, default to 256 MB
   */
  static final int DEFAULT_DOWNLOAD_SIZE = 1024 * 1024 * 256;

  private final AmazonS3 s3Client;
  private final S3BlockCache cache;

  public S3CachedReader(AmazonS3 s3Client) {
    this.s3Client = s3Client;
    this.cache = S3BlockCache.getInstance();
  }

  protected int read(S3ObjectSummary summary, ByteBuffer b, long startOffset, int length) throws IOException {
    LOG.info("[read] " + summary + " @" + startOffset + ":" + length);
    List<S3FileBlock> fileBlocks = S3FileBlock.of(summary, startOffset, length);
    final Map<S3FileBlock, byte[]> cacheBlocks = new HashMap<>();
    fileBlocks.forEach(block -> cacheBlocks.put(block, cache.getBlock(block)));

    MinMaxPriorityQueue<S3FileBlock> cacheMisses = MinMaxPriorityQueue.create();
    for (S3FileBlock block : fileBlocks) {
      byte[] data = cacheBlocks.get(block);
      if (data == null) {
        cacheMisses.add(block);
      } else {
        read(block, data, b, startOffset, length);
      }
    }

    if (!cacheMisses.isEmpty()) {
      long downloadStartOffset = cacheMisses.peekFirst().offset;
      long downloadEndOffset = Math.min(summary.getSize(), cacheMisses.peekLast().offset + DEFAULT_DOWNLOAD_SIZE);
      int downloadLength = Math.toIntExact(downloadEndOffset - downloadStartOffset);
      List<S3FileBlock> downloadBlocks = S3FileBlock.of(summary, downloadStartOffset, downloadLength);

      LOG.info("[readFromS3] " + summary + " @" + downloadStartOffset + ":" + downloadLength);
      GetObjectRequest rangeObjectRequest = new GetObjectRequest(summary.getBucketName(), summary.getKey())
          .withRange(downloadStartOffset, downloadEndOffset - 1);
      S3Object object = s3Client.getObject(rangeObjectRequest);
      ReadableByteChannel channel = Channels.newChannel(object.getObjectContent());

      for (S3FileBlock block : downloadBlocks) {
        byte[] data = new byte[block.length()];
        ByteBuffer buf = ByteBuffer.wrap(data);
        int bytesRead = IOUtils.read(channel, buf);
        assert bytesRead == block.length();
        cache.cacheBlock(block, data);

        if (block.offset < startOffset + length) {
          read(block, data, b, startOffset, length);
        }
      }

      object.close();
    }

    return length;
  }

  private void read(S3FileBlock block, byte[] data, ByteBuffer b, long startOffset, int length) throws IOException {
    long endOffset = startOffset + length;
    InputStream is = new ByteArrayInputStream(data);
    ReadableByteChannel ch = Channels.newChannel(is);
    long blockStart = block.offset;
    long blockEnd = block.offset + block.length();

    if (blockStart < startOffset) {
      long skipped = is.skip(startOffset - blockStart);
      assert skipped == startOffset - blockStart;
    }

    int toRead = Math.toIntExact(Math.min(blockEnd, endOffset) - Math.max(blockStart, startOffset));
    int position = Math.max(0, Math.toIntExact(blockStart - startOffset));

    b.limit(position + toRead).position(position);

    int bytesRead = IOUtils.read(ch, b);
    assert bytesRead == toRead;
  }
}
