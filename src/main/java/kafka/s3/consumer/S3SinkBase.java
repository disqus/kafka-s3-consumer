package kafka.s3.consumer;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Observer;
import java.util.UUID;

import kafka.s3.UploadObserver;

import org.slf4j.LoggerFactory;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

public class S3SinkBase {
	private static final org.slf4j.Logger logger = LoggerFactory
			.getLogger(App.class);

	private String bucket;
	private AmazonS3Client awsClient;
	private String keyPrefix;
	private int uploads;
	private UploadObserver obs;
	private int partition;
	private DateFormat dateFormat;
	private String topic;
	PropertyConfiguration conf;

	protected Map<String, Integer> topicSizes;

	public S3SinkBase(String topic, int partition, PropertyConfiguration conf) {
		super();

		obs = new UploadObserver();
		this.partition = partition;
		this.conf = conf;
		this.topic = topic;

    dateFormat = new SimpleDateFormat(conf.getS3TimePartitionFormat());

		bucket = conf.getS3Bucket();
		awsClient = new AmazonS3Client(new BasicAWSCredentials(
				conf.getS3AccessKey(), conf.getS3SecretKey()));

		topicSizes = conf.getTopicSizes();
		uploads = 0;
	}

	public void addObserver(Observer o) {
		obs.addObserver(o);
	}

	private String getKeyPrefix() {
		Date date = new Date();
    return getKeyPrefix(new Date());
	}

	private String getKeyPrefix(Date date) {
		return "%s/category=%s/%s/%s".format(conf.getS3Prefix(), topic,
				dateFormat.format(date), partition);
	}

	protected void commitChunk(File chunk, long startOffset, long endOffset) {
		logger.info("Uploading chunk to S3.");
		String key = getKeyPrefix() + "_" + System.currentTimeMillis() / 1000
				+ "_" + startOffset + "_" + endOffset + "_" + UUID.randomUUID()
				+ ".gz";
		awsClient.putObject(bucket, key, chunk);
		uploads++;
		obs.incrUploads();
	}

  protected void commitChunk(File chunk, long startOffset, long endOffset, Date date) {
    logger.info("Uploading chunk to S3.");
    String key = "%s-%s-%s-%s".format(getKeyPrefix(date), startOffset, endOffset, UUID.randomUUID());
  }

	public int getUploads() {
		return uploads;
	}

}
