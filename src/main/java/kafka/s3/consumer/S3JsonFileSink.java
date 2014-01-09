package kafka.s3.consumer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;
import java.util.zip.GZIPOutputStream;


import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import org.slf4j.LoggerFactory;
import org.apache.commons.lang.time.DateUtils;


class S3JsonFileSink extends S3SinkBase implements Sink {

	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(App.class);

  private final Long fileLease = 30000L;

	private Integer s3MaxObjectSize;
	private Integer bytesWritten;
	private Long startOffset;
	private Long endOffset;
  private Date partitionDate;
  private Long timestamp;

	ByteBuffer buffer;
	GZIPOutputStream goutStream;

	File tmpFile;
	OutputStream tmpOutputStream;
	OutputStream writer;
	String topic;

	private GZIPOutputStream getOutputStream(File tmpFile)
			throws FileNotFoundException, IOException {
		logger.debug("Creating gzip output stream for tmpFile: " + tmpFile);
		return new GZIPOutputStream(new FileOutputStream(tmpFile));
	}

	public S3JsonFileSink(String topic, int partition, PropertyConfiguration conf) throws IOException {
		super(topic, partition, conf);

    timestamp = System.currentTimeMillis();

		this.topic = topic;
    startOffset = 0L;
    endOffset = 0L;
    bytesWritten = 0;

		if (!topicSizes.containsKey(topic)) {
			logger.warn("No topic specific size found for topic: " + topic);
			s3MaxObjectSize = conf.getS3MaxObjectSize();
		} else {
			s3MaxObjectSize = topicSizes.get(topic);
		}
	}

  public void checkFileLease() {
    Long now = System.currentTimeMillis();
    if (partitionDate != null && now - timestamp > fileLease) {
      logger.debug("File lease expired for {}", partitionDate);
      prepareAndCommitFileStream(partitionDate);
      timestamp = now;
    }
  }

  private void prepareAndCommitFileStream(Date date) {
    logger.debug("Preparing file stream for partition {}", date);
      try {
        if (goutStream != null) {
          goutStream.close();
          commitChunk(tmpFile, startOffset, endOffset, date);
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

    // Grab the timestamp (first 8 bytes)
    Date messagePartitionDate = DateUtils.truncate(new Date(buffer.getLong()*1000), Calendar.HOUR);
		int messageSize = msgAndMetadata.message().payload().remaining();

    // Load message into the byte[]
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);

    if (partitionDate == null
        || !partitionDate.equals(messagePartitionDate)
        || bytesWritten + messageSize > s3MaxObjectSize) {
      if (partitionDate == null)
        partitionDate = messagePartitionDate;
      prepareAndCommitFileStream(partitionDate);
		}

    partitionDate = messagePartitionDate;
		goutStream.write(bytes);
		goutStream.write('\n');
		bytesWritten += messageSize;
		endOffset++;
		return messageSize;
	}

}
