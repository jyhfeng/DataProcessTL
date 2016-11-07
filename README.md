# DataProcessTL
This is a simple use module to process data with multi threads.<br/>
You will need :<br/>
  * JDK 1.8
  * log4j2
  
###Purpose:
  Sometimes we want to increase speed with processing data. We will use multi-threads to handle that.<br/>
  But deal with multi-threads well is not easy,especially rookies.<br/>
  In order to solve this,write this module.<br/>
  
  There are two types : Single & Batch (they are all multi-threads)<br/>
  The differents is: <br/>  
  
######Single Type:<br/>
  every thread do its job,when job is done,process flush();<br/>
  
######Batch Type:<br/>
  every thread do its job,when job is done,processed data will throw into a collection to wait,until times'up,<br/>
  or reach the number you set,process flush();<br/>
  
As so far ,I believe everyone still confusing what I said, that' fine. 
et's see how to coding and reached your target.

We will demo a simple program,an addition from 1 to 1000(1+2+3+....+1000)   
1 create your class to extend DataProcess

    public class SampleConsumer implement DataConsumer {
    	@Override
    	public void service(Object obj, Map params) throws Exception {
   		this.getDataQueue().add(obj);
    	}
   }
2 create your class to extend DataProcess

    public class SampleProcess extends DataProcess {
     private long count = 0;
     @Override
     public void flush(Queue dataQueue, Map params) throws Exception {
        int queueSize = dataQueue.size();
        IntStream.range(0, queueSize).forEach(v -> count += (int) dataQueue.poll());
        System.out.println("current data sum:" + count);
     }
    }
3 set config and run it

    List store = new ArrayList();
    IntStream.range(0, 1000).forEach(v -> store.add(v));
    //create params
    Map params = new HashMap();
    params.put("key1", "value1");
    params.put("key2", "value2");
    try {
        System.out.println("~~~START~~~");
        String tag = "test";
        DataQueueCenter dqc = new DataQueueCenter();
        dqc.createQueue(tag, null);
    
        //add data to queue
        dqc.addData(tag, store.toArray());
        
        //new class which extends DataProcess
        DataFlush dataFlush = new DataFlush();
        dataFlush.setConsumerClassName(BatchDataConsumer.class.getName());
        dataFlush.setDataQueueCenter(dqc);
        dataFlush.setTag(tag);
        dataFlush.setThreadType(ThreadType.BATCH_TYPE);
        dataFlush.setParams(params);
        dataFlush.setAddFail(false);
        dataFlush.setThreadNumber(4);
        dataFlush.setSleepTime(10);
        dataFlush.setReleaseCpuControlLimit(100);
        
        //start multi threads to process
        dataFlush.start();
        //make a sleep
        Thread.sleep(3000);
        //send end tag to tell service ready to end the service
        dataFlush.sendEndTag();
        //when you process this method,the service will wait,until the data were processed.
        dataFlush.waitTaskDone();
    } catch (Exception e) {
        e.printStackTrace();
    }
    System.out.println("~~~END~~~");
