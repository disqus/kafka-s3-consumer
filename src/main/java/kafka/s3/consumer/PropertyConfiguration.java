package kafka.s3.consumer;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;


public class PropertyConfiguration implements Configuration {

	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(PropertyConfiguration.class);

  private CompositeConfiguration config = new CompositeConfiguration();

	public PropertyConfiguration(URL propsLocation) throws ConfigurationException {
    config.addConfiguration(new SystemConfiguration());
    config.addConfiguration(new PropertiesConfiguration(propsLocation));
		if (config.isEmpty()) {
			throw new RuntimeException("Empty config");
		}
	}

  public String getString(String s) {
    return config.getString(s);
  }

  public String getString(String s, String d) {
    return config.getString(s, d);
  }

	public int getInt(String s) {
		return config.getInt(s);
	}

	private Map<String, Integer> getConfigMap(String prop) {
		HashMap<String, Integer> result = new HashMap<String, Integer>();
		String[] fields = config.getStringArray(prop);
		for (String topics : fields) {
			String[] fieldPart = topics.trim().split(":");
			if (result.containsKey(fieldPart[0])) {
				throw new RuntimeException("Duplicate field " + fieldPart[0]);
			}
			result.put(fieldPart[0], Integer.valueOf(fieldPart[1]));
		}
		return result;
	}

	protected String getS3AccessKey() {
		String s3AccessKey = config.getString(PROP_S3_ACCESS_KEY);
		if (s3AccessKey == null || s3AccessKey.isEmpty()) {
			throw new RuntimeException("Invalid property " + PROP_S3_ACCESS_KEY);
		}
		return s3AccessKey;
	}

	protected String getS3SecretKey() {
		String s3SecretKey = config.getString(PROP_S3_SECRET_KEY);
		if (s3SecretKey == null || s3SecretKey.isEmpty()) {
			throw new RuntimeException("Invalid property " + PROP_S3_SECRET_KEY);
		}
		return s3SecretKey;
	}

	protected String getS3Bucket() {
		String s3Bucket = config.getString(PROP_S3_BUCKET);
		if (s3Bucket == null || s3Bucket.isEmpty()) {
			throw new RuntimeException("Invalid property " + PROP_S3_BUCKET);
		}
		return s3Bucket;
	}

	protected String getS3Prefix() {
		String s3Prefix = config.getString(PROP_S3_PREFIX);
		if (s3Prefix == null || s3Prefix.isEmpty()) {
			throw new RuntimeException("Invalid property " + PROP_S3_PREFIX);
		}
		return s3Prefix.replaceAll("/$", "");
	}

	protected Map<String, Integer> getTopicsAndPartitions() {
		return getConfigMap(PROP_KAFKA_TOPICS);
	}

	public int getS3MaxObjectSize() {
		return config.getInt(PROP_S3_MAX_OBJECT_SIZE, DEFAULT_S3_SIZE);
	}

	public int getKafkaMaxMessageSize() {
		return config.getInt(PROP_KAFKA_MAX_MESSAGE_SIZE, DEFAULT_MSG_SIZE);
	}

	protected Map<String, Integer> getTopicSizes() {
		logger.debug("Topic size map {}", getConfigMap(PROP_S3_TOPIC_SIZES).keySet());
		return getConfigMap(PROP_S3_TOPIC_SIZES);
	}

  protected String getS3TimePartitionFormat() {
    return config.getString(PROP_S3_TIME_PARTITION_FORMAT, DEFAULT_S3_TIME_PARTITION_FORMAT);
  }

}

// vim: noet:ts=2:sw=2
