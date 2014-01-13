package kafka.s3.consumer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.zip.GZIPOutputStream;


import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import org.slf4j.LoggerFactory;


class S3JsonFileSink extends S3SinkBase implements Sink {

	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(App.class);

  private final Long fileLease = 300000L;  // 5 minutes

	private Integer s3MaxObjectSize;
	private Integer bytesWritten;
	private Long startOffset;
	private Long endOffset;
  private Date partitionDate;
  private Long timestamp;
	private Integer emptyCommits;

	GZIPOutputStream goutStream;

	File tmpFile;
	String topic;

	private GZIPOutputStream getOutputStream(File tmpFile)
			throws FileNotFoundException, IOException {
		logger.debug("Creating gzip output stream for tmpFile: " + tmpFile);
		return new GZIPOutputStream(new FileOutputStream(tmpFile));
	}

	public S3JsonFileSink(String topic, int partition, PropertyConfiguration conf, Date partitionDate) throws IOException {
		super(topic, partition, conf);

    timestamp = System.currentTimeMillis();

		this.topic = topic;
		this.partitionDate = partitionDate;
    startOffset = 0L;
    endOffset = 0L;
    bytesWritten = 0;
		emptyCommits = 0;

		if (!topicSizes.containsKey(topic)) {
			logger.warn("No topic specific size found for topic: " + topic);
			s3MaxObjectSize = conf.getS3MaxObjectSize();
		} else {
			s3MaxObjectSize = topicSizes.get(topic);
		}

		prepareAndCommitFileStream(partitionDate);
	}

  public void checkFileLease() {
    Long now = System.currentTimeMillis();
    if (now - timestamp > fileLease) {
      logger.debug("File lease expired for {}", partitionDate);
      prepareAndCommitFileStream(partitionDate);
      timestamp = now;
    }
  }

	public boolean isStale() {
		return emptyCommits >= 3;
	}

  private void prepareAndCommitFileStream(Date date) {
    logger.debug("Preparing file stream for partition {}", date);
      try {
        if (goutStream != null) {
          goutStream.close();
          if (tmpFile != null && bytesWritten != 0) {
            commitChunk(tmpFile, startOffset, endOffset, date);
						emptyCommits = 0;
          } else {
						emptyCommits++;
					}
        }
        if (tmpFile != null) {
          tmpFile.delete();
        }
        tmpFile = File.createTempFile("s3sink:%s".format(getTimePartition(date)), null);
        if (goutStream != null) {
          goutStream.finish();
        }
        goutStream = getOutputStream(tmpFile);
        startOffset = endOffset;
        bytesWritten = 0;
      } catch (IOException e) {
        throw new RuntimeException("Error with file streams.");
      }
  }

	@Override
	public long append(MessageAndMetadata<Message> msgAndMetadata) throws IOException {
		ByteBuffer buffer = msgAndMetadata.message().payload();

		// HACK: need to re-read the first 8 bytes again.
		buffer.getLong();
		int messageSize = msgAndMetadata.message().payload().remaining();

    // Load message into the byte[]
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);

		if (bytesWritten + messageSize > s3MaxObjectSize) {
			prepareAndCommitFileStream(partitionDate);
		}

		goutStream.write(bytes);
		goutStream.write('\n');
		bytesWritten += messageSize;
		endOffset++;
		return messageSize;
	}

	public void close() {
		if (tmpFile != null) {
			tmpFile.delete();
		}

		super.close();
	}
}

// vim: noet:ts=2:sw=2
