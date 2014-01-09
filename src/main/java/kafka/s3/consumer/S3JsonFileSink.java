package kafka.s3.consumer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.zip.GZIPOutputStream;

import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import org.slf4j.LoggerFactory;


class S3JsonFileSink extends S3SinkBase implements Sink {

	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(App.class);

	private Integer s3MaxObjectSize;
	private Integer bytesWritten;
	private Long startOffset;
	private Long endOffset;
  private String timePartition;

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

		this.topic = topic;

		if (!topicSizes.containsKey(topic)) {
			logger.warn("No topic specific size found for topic: " + topic);
			s3MaxObjectSize = conf.getS3MaxObjectSize();
		} else {
			s3MaxObjectSize = topicSizes.get(topic);
		}
	}

  public void prepareAndCommitFileStream(Date date) {
      try {
        if (goutStream != null) {
          goutStream.close();
          commitChunk(tmpFile, startOffset, endOffset, date);
        }
        if (tmpFile != null)
          tmpFile.delete();
        tmpFile = File.createTempFile("s3sink:{}".format(timePartition), null);
        if (goutStream != null)
          goutStream.finish();
        goutStream = getOutputStream(tmpFile);
        if (endOffset == null) {
          startOffset = 0L;
          endOffset = 0L;
        } else {
          startOffset = endOffset;
        }
        bytesWritten = 0;
      } catch (IOException e) {
        throw new RuntimeException("Error with file streams.");
      }
  }

	@Override
	public long append(MessageAndMetadata<Message> msgAndMetadata) throws IOException {
		ByteBuffer buffer = msgAndMetadata.message().payload();

    // Grab the timestamp (first 8 bytes)
    Date date = new Date(buffer.getLong()*1000);
    String messageTimePartition = getTimePartition(date);
		int messageSize = msgAndMetadata.message().payload().remaining();

    // Load message into the byte[]
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);

    if (tmpFile == null
        || messageTimePartition != timePartition
        || bytesWritten + messageSize > s3MaxObjectSize) {
      prepareAndCommitFileStream(date);
		}

    timePartition = messageTimePartition;
		goutStream.write(bytes);
		goutStream.write('\n');
		bytesWritten += messageSize;
		endOffset++;
		return messageSize;
	}

}
