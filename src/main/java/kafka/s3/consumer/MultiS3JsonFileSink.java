package kafka.s3.consumer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;

import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import org.apache.commons.lang.time.DateUtils;
import org.slf4j.LoggerFactory;

class MultiS3JsonFileSink implements Sink, Observer {
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(App.class);

	private Map<PartitionKey, S3JsonFileSink> fileSinkPartitions;
	private String topic;
	private int partition;
	private int uploads;
	private PropertyConfiguration conf;
	private Observer obs;


	public MultiS3JsonFileSink(String topic, int partition, PropertyConfiguration conf) throws IOException {
		this.topic = topic;
		this.partition = partition;
		this.conf = conf;

		this.fileSinkPartitions = new HashMap<PartitionKey, S3JsonFileSink>();
	}

	public long append(S3ConsumerProtos.Message message) throws IOException {
		PartitionKey partKey = new PartitionKey(message.getPath());

		S3JsonFileSink sink = fileSinkPartitions.get(partKey);

		if (sink == null) {
			logger.info("Creating new S3JsonFileSync for partition: {}", partKey);
			sink = new S3JsonFileSink(topic, partition, conf, partKey);
			sink.addObserver(obs);
			sink.addObserver(this);
			fileSinkPartitions.put(partKey, sink);
		}

		return sink.append(message);
	}

	public void addObserver(Observer o) {
		this.obs = o;
	}

	public void checkFileLease() {
		ArrayList<PartitionKey> toRemove = new ArrayList<PartitionKey>();

		for (Map.Entry<PartitionKey, S3JsonFileSink> entry : fileSinkPartitions.entrySet()) {
			S3JsonFileSink sink = entry.getValue();
			sink.checkFileLease();
			if (sink.isStale()) {
				sink.close();
				toRemove.add(entry.getKey());
			}
		}

		for (PartitionKey key : toRemove) {
			logger.debug("Removing stale partition: {}", key);
			fileSinkPartitions.remove(key);
		}
	}

	public int getUploads() {
		return uploads;
	}

	@Override
	public void update(Observable obs, Object arg) {
		uploads++;
	}
}

// vim: noet:ts=2:sw=2
