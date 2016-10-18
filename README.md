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
  
