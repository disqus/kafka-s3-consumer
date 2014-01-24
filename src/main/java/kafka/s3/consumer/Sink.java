package kafka.s3.consumer;

import java.io.IOException;
import java.util.Date;
import java.util.Observer;

interface Sink {

	public long append(S3ConsumerProtos.Message message)
			throws IOException;
	public void addObserver(Observer o);
	public void checkFileLease();
	public int getUploads();
}

// vim: noet:ts=2:sw=2
