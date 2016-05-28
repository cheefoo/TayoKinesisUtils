package com.tayo.TayoKinesisUtils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;

public class SinglePuts implements Runnable
{

	public void run() 
	{
		AmazonKinesisClient kinesis = new AmazonKinesisClient(new ProfileCredentialsProvider()
    			.getCredentials()).withRegion(Regions.US_EAST_1);
		
		try {
			singlePut(kinesis);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	

	private static void singlePut(AmazonKinesisClient kinesis) throws UnsupportedEncodingException 
	{
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < 100000; i++)
    	{
    		long createTime = System.currentTimeMillis();
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(Utils.myStreamName);
            //put random data
            putRecordRequest.setData(ByteBuffer.wrap(String.format(i+"single", createTime).getBytes("UTF-8")));
            putRecordRequest.setPartitionKey(Utils.randomExplicitHashKey());
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
}
