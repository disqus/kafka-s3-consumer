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

	private Map<Date, S3JsonFileSink> fileSinkPartitions;
	private String topic;
	private int partition;
	private int uploads;
	private PropertyConfiguration conf;
	private Observer obs;


	public MultiS3JsonFileSink(String topic, int partition, PropertyConfiguration conf) throws IOException {
		this.topic = topic;
		this.partition = partition;
		this.conf = conf;

		this.fileSinkPartitions = new HashMap<Date, S3JsonFileSink>();
	}

	public long append(MessageAndMetadata<Message> msgAndMetadata) throws IOException {
		ByteBuffer buffer = msgAndMetadata.message().payload();

		// Grab the timestamp (first 8 bytes)
		Date messagePartitionDate = DateUtils.truncate(new Date(buffer.getLong() * 1000), Calendar.HOUR);

		S3JsonFileSink sink = fileSinkPartitions.get(messagePartitionDate);

		if (sink == null) {
			logger.info("Creating new S3JsonFileSync for partition: {}", messagePartitionDate);
			sink = new S3JsonFileSink(topic, partition, conf, messagePartitionDate);
			sink.addObserver(obs);
			sink.addObserver(this);
			fileSinkPartitions.put(messagePartitionDate, sink);
		}

		return sink.append(msgAndMetadata);
	}

	public void addObserver(Observer o) {
		this.obs = o;
	}

	public void checkFileLease() {
		ArrayList<Date> toRemove = new ArrayList<Date>();

		for (Map.Entry<Date, S3JsonFileSink> entry : fileSinkPartitions.entrySet()) {
			S3JsonFileSink sink = entry.getValue();
			sink.checkFileLease();
			if (sink.isStale()) {
				sink.close();
				toRemove.add(entry.getKey());
			}
		}

		for (Date d : toRemove) {
			logger.debug("Removing stale partition: {}", d);
			fileSinkPartitions.remove(d);
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
