package kafka.s3.consumer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
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
	private PartitionKey partitionKey;
  private Long timestamp;
	private Integer emptyCommits;
	private DateFormat dateFormat;

	GZIPOutputStream goutStream;

	File tmpFile;
	String topic;

	private GZIPOutputStream getOutputStream(File tmpFile)
			throws FileNotFoundException, IOException {
		logger.debug("Creating gzip output stream for tmpFile: " + tmpFile);
		return new GZIPOutputStream(new FileOutputStream(tmpFile));
	}

	public S3JsonFileSink(String topic, int partition, PropertyConfiguration conf, PartitionKey partitionKey) throws IOException {
		super(topic, partition, conf);

    timestamp = System.currentTimeMillis();

		this.topic = topic;
		this.partitionKey = partitionKey;
    startOffset = 0L;
    endOffset = 0L;
    bytesWritten = 0;
		emptyCommits = 0;

		dateFormat = new SimpleDateFormat(conf.getS3TimePartitionFormat());

		if (!topicSizes.containsKey(topic)) {
			logger.warn("No topic specific size found for topic: " + topic);
			s3MaxObjectSize = conf.getS3MaxObjectSize();
		} else {
			s3MaxObjectSize = topicSizes.get(topic);
		}

		prepareAndCommitFileStream();
	}

  public void checkFileLease() {
    Long now = System.currentTimeMillis();
    if (now - timestamp > fileLease) {
      logger.debug("File lease expired for {}", partitionKey);
      prepareAndCommitFileStream();
      timestamp = now;
    }
  }

	public boolean isStale() {
		return emptyCommits >= 3;
	}


	// TODO: fix this.
	private String getTopicName() {
		if (prefix != null) {
			return topic.substring(prefix.length());
		} else {
			return topic;
		}
	}

	private String getTimePartition() {
		return dateFormat.format(partitionKey.getDate());
	}

	private String getKey() {
		String path = String.format("%s/category=%s/%s", conf.getS3Prefix(),
				getTopicName(), getTimePartition());

		String extraPartition = partitionKey.getExtraPath();
		if (!extraPartition.isEmpty()) {
			extraPartition += "/";
		}

		String filename = String.format("%d:%d:%d:%s.gz", partition,
				startOffset, endOffset, UUID.randomUUID());

		String key = path + "/" + extraPartition + filename;
		System.out.println("key: " + key);

		return key;
	}

  private void prepareAndCommitFileStream() {
    logger.debug("Preparing file stream for partition {}", partitionKey);
    System.out.println("Preparing file stream for partition " + partitionKey);
      try {
        if (goutStream != null) {
          goutStream.close();
          if (tmpFile != null && bytesWritten != 0) {
            commitChunk(tmpFile, getKey());
						emptyCommits = 0;
          } else {
						emptyCommits++;
					}
        }
        if (tmpFile != null) {
          tmpFile.delete();
        }
				// TODO: This isn't doing what is expected... doesn't really matter
				// but should get rid of it if it doesn't work.
        tmpFile = File.createTempFile(
            "%s_%s_%s".format(getTopicName(), getTimePartition(), partitionKey.getExtraPath().replace('/', '_')),
            null);
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
	public long append(S3ConsumerProtos.Message message) throws IOException {
		byte[] bytes = message.getData().getBytes();
		int messageSize = bytes.length;

		if (bytesWritten + messageSize > s3MaxObjectSize) {
			prepareAndCommitFileStream();
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
