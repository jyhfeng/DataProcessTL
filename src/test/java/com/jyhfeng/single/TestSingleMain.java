package com.jyhfeng.single;

import com.jyhfeng.queue.DataQueueCenter;
import com.jyhfeng.queue.ThreadType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

/**
 * Created by dl85 on 2015/3/3.
 */
public class TestSingleMain {
    public static void main(String[] args) {

        List store = new ArrayList();
        IntStream.range(0,10000).forEach(v->{
            store.add(v);
        });

        try {
            System.out.println("~~~START~~~");
            String tag = "test";
            //add data to queue
            DataQueueCenter dataQueueCenter = new DataQueueCenter();
            dataQueueCenter.createQueue(tag, null);
            dataQueueCenter.addData(tag, store.toArray());

            //new class which extends DataProcess
            DataFlush dataFlush = new DataFlush();
            dataFlush.setConsumerClassName(SingleDataConsumer.class.getName());
            dataFlush.setDataQueueCenter(dataQueueCenter);
            dataFlush.setTag(tag);
            dataFlush.setThreadType(ThreadType.SINGLE_TYPE);
            //start multi threads to process
            dataFlush.start();

            //send end tag to tell service ready to end the service
            dataFlush.sendEndTag();
            //when you process this method,the service will wait,until the data were processed.
            //dataFlush.waitTaskDone();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("~~~END~~~");
    }
}
