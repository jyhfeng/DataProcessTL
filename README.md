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
Let's see how to coding and reached your target.

We will demo a simple program,an addition from 1 to 1000(1+2+3+....+1000)   
1. create your class to extend DataProcess

    public class BatchDataConsumer extends DataConsumer {
    @Override
    public void service(Object obj, Map params) throws Exception {
        this.getDataQueue().add(obj);
    }
    }
	
2. create your class to implement DataConsumer

	

3. set config and run it
