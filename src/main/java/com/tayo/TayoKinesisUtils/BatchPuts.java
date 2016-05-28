package com.tayo.TayoKinesisUtils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;

public class BatchPuts implements Runnable 
{

	public void run() 
	{
		AmazonKinesisClient kinesis = new AmazonKinesisClient(new ProfileCredentialsProvider()
    			.getCredentials()).withRegion(Regions.US_EAST_1);
		
		try {
			batchPuts(kinesis);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public  void batchPuts(AmazonKinesisClient kinesis) throws UnsupportedEncodingException 
    {
    	long createTime = System.currentTimeMillis();
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(Utils.myStreamName);
       
        List<PutRecordsRequestEntry> ptreList = new ArrayList<PutRecordsRequestEntry>();
        int batch = 1;
        long startTime = System.currentTimeMillis();
        for (int i = 1; i < 100001; i++)
        {
        	PutRecordsRequestEntry ptre = new PutRecordsRequestEntry();
        	//Put random data
        	ptre.setData(ByteBuffer.wrap(String.format(i+"batch", createTime).getBytes("UTF-8")));    
        	ptre.setPartitionKey(Utils.randomExplicitHashKey());
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
