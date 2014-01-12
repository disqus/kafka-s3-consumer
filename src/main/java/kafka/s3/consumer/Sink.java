package kafka.s3.consumer;

import java.io.IOException;
import java.util.Date;
import java.util.Observer;

import kafka.message.Message;
import kafka.message.MessageAndMetadata;

interface Sink {

	public long append(MessageAndMetadata<Message> messageAndOffset)
			throws IOException;
	public void addObserver(Observer o);
	public void checkFileLease();
	public long getCommitCount();
}

// vim: noet:ts=2:sw=2
