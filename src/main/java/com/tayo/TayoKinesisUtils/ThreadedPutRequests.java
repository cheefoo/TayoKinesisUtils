package com.tayo.TayoKinesisUtils;

public class ThreadedPutRequests 
{
	public static void main (String [] args) throws InterruptedException
	{
		Thread single = new Thread(new SinglePuts());
		single.start();
		System.out.println("Single put Record Request started");
		Thread batch = new Thread(new BatchPuts());
		batch.start();
		System.out.println("Batch put Records Request started");
		
	}

}
