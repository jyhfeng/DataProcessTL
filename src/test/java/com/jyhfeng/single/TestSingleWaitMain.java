package com.jyhfeng.single;

import com.jyhfeng.queue.DataQueueCenter;
import com.jyhfeng.queue.ThreadType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Created by dl85 on 2015/3/4.
 */
public class TestSingleWaitMain {
    public static void main(String[] args) {
        //create test data
        List store = new ArrayList();
        IntStream.range(0,10000).forEach(v->store.add(v));
        List store2 = new ArrayList(store);

        try {
            System.out.println("~~~START~~~");
            String tag = "test";

            DataQueueCenter dataQueueCenter=new DataQueueCenter();
            dataQueueCenter.createQueue(tag, null);
            //add data to queue
            dataQueueCenter.addData(tag, store.toArray());

            //new class which extends DataProcess
            DataFlush dataFlush = new DataFlush();
            dataFlush.setConsumerClassName(SingleDataConsumer.class.getName());
            dataFlush.setDataQueueCenter(dataQueueCenter);
            dataFlush.setTag(tag);
            dataFlush.setThreadType(ThreadType.SINGLE_TYPE);
            //process fail data - need to overwrite flushFail
            dataFlush.setAddFail(true);
            //start multi threads to process
            dataFlush.start();

            //make a sleep
            Thread.sleep(5000);
            //add data to queue again
            dataQueueCenter.addData(tag, store2.toArray());

            //send end tag to tell service ready to end the service
            dataFlush.sendEndTag();
            //when you process this method,the service will wait,until the data were processed.
            dataFlush.waitTaskDone();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("~~~END~~~");
    }
}
