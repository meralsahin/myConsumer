package com.meral.kinesisconsumer;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RecordProcessor implements IRecordProcessor {
	
	//it receives the records from the stream and processes 
	
	
	//writing to log for debugging
	private static final Log LOG = LogFactory.getLog(RecordProcessor.class);
	private String kinesisShardId;
	
	// Backoff and retry settings-you wait for this long until retry
    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final int NUM_RETRIES = 10;
    
    
    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
    
    // Checkpoint about once a minute
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
    private long nextCheckpointTimeInMillis;
    
    ObjectMapper mapper = new ObjectMapper();
    
    //Writing to DynamoDB stuff:
    private static AWSCredentials credentials = null;
    
    
    static DynamoDB dynamoDB;

	@Override
	public void initialize(String shardId) {
		this.kinesisShardId = shardId;
		try {
	        credentials = new ProfileCredentialsProvider().getCredentials();
	    } catch (Exception e) {
	        throw new AmazonClientException(
	                "Cannot load the credentials from the credential profiles file. " +
	                "Please make sure that your credentials file is at the correct " +
	                "location (~/.aws/credentials), and is in valid format.",
	                e);
	    }
		dynamoDB = new DynamoDB(new AmazonDynamoDBClient(
	            credentials));
	}

	@Override
	public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
		 // Process records and perform all exception handling.
        processRecordsWithRetries(records);

        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }
	}
	
	/**
     * Process records performing retries as needed. Skip "poison pill" records.
     * 
     * @param records Data records to be processed.
     */
    private void processRecordsWithRetries(List<Record> records) {
        for (Record record : records) {
            boolean processedSuccessfully = false;
            for (int i = 0; i < NUM_RETRIES; i++) {
                try {
                    //
                    // Logic to process record goes here.
                    //
                    processSingleRecord(record);

                    processedSuccessfully = true;
                    break;
                } catch (Throwable t) {
                    LOG.debug("Caught throwable while processing record " + record, t);
                }

                // Backoff if encounter an exception.
                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                } catch (InterruptedException e) {
                    LOG.debug("Interrupted sleep", e);
                }
            }

            if (!processedSuccessfully) {
                LOG.error("Couldn't process record " + record + ". Skipping the record.");
            }
        }
    }
    
    /**
     * Process a single record.
     * 
     * @param record The record to be processed.
     * @throws IOException 
     * @throws JsonMappingException 
     * @throws JsonParseException 
     */
    private void processSingleRecord(Record record) throws JsonParseException, JsonMappingException, IOException {

    	System.out.println("received record: " + record + "\ndata: " + record.getData() +
        		", pk: " + record.getPartitionKey() + ", sequnce number: " + record.getSequenceNumber());
        String data = null;
        try {
            // convert the stream to UTF-8 chars.
            data = decoder.decode(record.getData()).toString();
            System.out.println("DATA: " + data);
            Map<String,Object> map = mapper.readValue(data, Map.class);
            //System.out.println("Long value is: " + map.get("timestampvalue"));
            System.out.println("myName is: " + map.get("myname"));
            System.out.println("myDate is: " + map.get("mydate"));
            System.out.println("myIP is: " + map.get("myIP"));
            System.out.println("myUserAgent is: " + map.get("userAgent"));
            System.out.println("myGUID is: " + map.get("guid"));
            
            System.out.println("myDate is of type: " + map.get("mydate").getClass().getCanonicalName());
            
            //1- Now the log data (timestamp and nameparameter) are available, write them to the DynamoDB table
            //2- The next step is to process the user data to caclucalte the stats, this is done with hive
            //   and map-reduce
            Table table = dynamoDB.getTable("my_application_consumer_table");
            Item item = new Item().withPrimaryKey("myName",map.get("myname")).with("myDate", map.get("mydate"))
            		.with("myIP", map.get("myIP")).with("myUserAgent", map.get("userAgent")).with("myGUID", map.get("guid"));
            table.putItem(item);
            System.out.println("\n\nITEM PUT IN TABLE   my_application_consumer_table  !!!!!!!!!!!!");

        } catch (NumberFormatException e) {
        	System.out.println("NUMBER ERROR!");
            LOG.info("Record does not match sample record format. Ignoring record with data; " + data);
        } catch (CharacterCodingException e) {
            LOG.error("Malformed data: " + data, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
    }
	
    /** Checkpoint with retries.
     * @param checkpointer
     */
    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Checkpointing shard " + kinesisShardId);
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                LOG.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (NUM_RETRIES - 1)) {
                    LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + NUM_RETRIES, e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                LOG.debug("Interrupted sleep", e);
            }
        }
    }
}