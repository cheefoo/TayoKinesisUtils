package com.tayo.TayoKinesisUtils;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;

public class Utils 
{
	private static final Random RANDOM = new Random();
	final static String myStreamName = "ConsoleStream";
    
    /**
     * @return A random unsigned 128-bit int converted to a decimal string.
     */
    public static String randomExplicitHashKey() 
    {
        return new BigInteger(128, RANDOM).toString(10);
    }
    
   
    public static void main (String [] args) throws UnsupportedEncodingException
    {
    	AmazonKinesisClient kinesis = new AmazonKinesisClient(new ProfileCredentialsProvider()
    			.getCredentials()).withRegion(Regions.US_EAST_1);
    	
    	
    	//singlePut(kinesis);
    	batchPuts(kinesis);
    	
    	System.out.println("Done");
    }

	private static void singlePut(AmazonKinesisClient kinesis) throws UnsupportedEncodingException 
	{
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < 100000; i++)
    	{
    		long createTime = System.currentTimeMillis();
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(myStreamName);
            //put random data
            putRecordRequest.setData(ByteBuffer.wrap(String.format(i+"single", createTime).getBytes("UTF-8")));
            putRecordRequest.setPartitionKey(randomExplicitHashKey());
            PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);
            System.out.printf("Successfully put record, partition key : %s, ShardID : %s, SequenceNumber : %s.\n",
                    putRecordRequest.getPartitionKey(),
                    putRecordResult.getShardId(),
                    putRecordResult.getSequenceNumber());
    		
    	}
		long endTime = System.currentTimeMillis();
		long timeTaken = endTime - startTime;
		System.out.println("Total time taken is " + timeTaken);
	}
    
    
    public static void batchPuts(AmazonKinesisClient kinesis) throws UnsupportedEncodingException 
    {
    	long createTime = System.currentTimeMillis();
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(myStreamName);
       
        List<PutRecordsRequestEntry> ptreList = new ArrayList<PutRecordsRequestEntry>();
        int batch = 1;
        long startTime = System.currentTimeMillis();
        for (int i = 1; i < 1001; i++)
        {
        	PutRecordsRequestEntry ptre = new PutRecordsRequestEntry();
        	//Put random data
        	ptre.setData(ByteBuffer.wrap(String.format(i+"batch", createTime).getBytes("UTF-8")));    
        	ptre.setPartitionKey(randomExplicitHashKey());
        	ptreList.add(ptre);
        	if((i%500) == 0)
        	{
        		System.out.println("Batch " + batch + " "+ "Request Size is " + ptreList.size());
        		putRecordsRequest.setRecords(ptreList);
        		PutRecordsResult putRecordsResult = kinesis.putRecords(putRecordsRequest);
        		
        		int failed = putRecordsResult.getFailedRecordCount();
        		System.out.println("Batch " + batch + " "+ " total failed record count  =" + failed);
        		System.out.println("Batch " + batch + " "+ "Result Size is " + putRecordsResult.getRecords().size() );
        		//Get Results from Kinesis
            	for (PutRecordsResultEntry entry: putRecordsResult.getRecords())
            	{
            		System.out.println(entry.toString());            		
            	}
            	batch++;
            	//reinitialize collection
            	ptreList = new ArrayList<PutRecordsRequestEntry>(); 
        	}
        	
        	 
        }
        long endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;
        System.out.println("Total Time taken is " + timeTaken);
        
	}
    	
    

}
