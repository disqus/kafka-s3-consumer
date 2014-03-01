package kafka.s3.consumer;

import java.io.File;
import java.util.Date;
import java.util.Map;
import java.util.Observer;

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
	protected int partition;
	protected String topic;
	PropertyConfiguration conf;

	protected Map<String, Integer> topicSizes;

	public S3SinkBase(String topic, int partition, PropertyConfiguration conf) {
		super();

		obs = new UploadObserver();
		this.partition = partition;
		this.conf = conf;
		this.topic = topic;

		bucket = conf.getS3Bucket();
		awsClient = new AmazonS3Client(new BasicAWSCredentials(
				conf.getS3AccessKey(), conf.getS3SecretKey()));

		topicSizes = conf.getTopicSizes();
		uploads = 0;
	}

	public void addObserver(Observer o) {
		obs.addObserver(o);
	}

	protected void commitChunk(File chunk, String key) {
    logger.debug("Uploading to s3 {}", key);
		awsClient.putObject(bucket, key, chunk);
		uploads++;
		obs.incrUploads();
	}

	public int getUploads() {
		return uploads;
	}

	public void close() {
		obs.deleteObservers();
	}
}

// vim: noet:ts=2:sw=2
