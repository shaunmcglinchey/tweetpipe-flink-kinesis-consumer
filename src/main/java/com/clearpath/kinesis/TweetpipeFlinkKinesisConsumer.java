package com.clearpath.kinesis;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.kinesis.shaded.com.amazonaws.auth.AWSCredentials;
import org.apache.flink.kinesis.shaded.com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.model.Record;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.util.Collector;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

public class TweetpipeFlinkKinesisConsumer {

	private static final String region = "eu-west-2";
	private static final String inputStream = "tweets-stream";

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		DefaultAWSCredentialsProviderChain credentialsProvider = new DefaultAWSCredentialsProviderChain();
		AWSCredentials credentials = credentialsProvider.getCredentials();

		Properties config = new Properties();
		config.setProperty(ConsumerConfigConstants.AWS_REGION, region);
		config.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, credentials.getAWSAccessKeyId());
		config.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, credentials.getAWSSecretKey());
		config.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "TRIM_HORIZON");

		DataStreamSource<Record> kinesis = env.addSource(new FlinkKinesisConsumer<>(
				inputStream,
				new RecordKinesisDeserializationSchema(),
				config)
		);

		kinesis.flatMap(new RecordToStatuses())
				.print();

		// execute program
		env.execute("TweetPipe Flink Kinesis Consumer");
	}

	private static class RecordKinesisDeserializationSchema implements KinesisDeserializationSchema<Record> {
		@Override
		public Record deserialize(byte[] recordValue,
								  String partitionKey,
								  String seqNum,
								  long approxArrivalTimestamp,
								  String stream,
								  String shardId) {
			return new Record()
					.withData(ByteBuffer.wrap(recordValue))
					.withPartitionKey(partitionKey)
					.withSequenceNumber(seqNum)
					.withApproximateArrivalTimestamp(new Date(approxArrivalTimestamp));
		}

		@Override
		public TypeInformation<Record> getProducedType() {
			return TypeInformation.of(Record.class);
		}
	}

	private static class RecordToStatuses implements FlatMapFunction<Record, Status> {

		@Override
		public void flatMap(Record record, Collector<Status> collector) throws Exception {
			for (UserRecord userRecord : UserRecord.deaggregate(Collections.singletonList(record))) {
				Status status = getStatus(userRecord);
				collector.collect(status);
			}
		}

		private Status getStatus(Record record) {
			ByteBuffer data = record.getData();
			String tweetJson = new String(data.array(), StandardCharsets.UTF_8);
			return parseTweet(tweetJson);
		}

		private Status parseTweet(String tweetJson) {
			try {
				return TwitterObjectFactory.createStatus(tweetJson);
			} catch (TwitterException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
