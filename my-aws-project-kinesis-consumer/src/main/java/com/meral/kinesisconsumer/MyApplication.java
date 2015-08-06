package com.meral.kinesisconsumer;

import java.net.InetAddress;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

public class MyApplication {

	public static final String APPLICATION_STREAM_NAME = "my_kinesis_stream";

	private static final String APPLICATION_NAME = "my_application_dynamodb_table";

	// Initial position in the stream when the application starts up for the first time.
	// Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest available data)
	private static final InitialPositionInStream APPLICATION_INITIAL_POSITION_IN_STREAM =
			InitialPositionInStream.LATEST;

	private static AWSCredentialsProvider credentialsProvider;

	private static void init() {
		//AWS stuff:
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. " +
							"Please make sure that your credentials file is at the correct " +
							"location (~/.aws/credentials), and is in valid format.",
							e);
		}
	}

	public static void main(String[] args) throws Exception {
		init();

		System.out.println("STARTING!!!!!");

		String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
		KinesisClientLibConfiguration kinesisClientLibConfiguration =
				new KinesisClientLibConfiguration(APPLICATION_NAME,
						APPLICATION_STREAM_NAME,
						credentialsProvider,
						workerId);
		kinesisClientLibConfiguration.withInitialPositionInStream(APPLICATION_INITIAL_POSITION_IN_STREAM);

		IRecordProcessorFactory recordProcessorFactory = new RecordProducerFactory();
		Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);

		System.out.printf("Running %s to process stream %s as worker %s...\n",
				APPLICATION_NAME,
				APPLICATION_STREAM_NAME,
				workerId);

		int exitCode = 0;
		try {
			worker.run();
		} catch (Throwable t) {
			System.err.println("Caught throwable while processing data.");
			t.printStackTrace();
			exitCode = 1;
		}
		System.exit(exitCode);
	}
}
