package batchmode;

import com.jyhfeng.queue.DataQueueCenter;
import com.jyhfeng.queue.ThreadType;

import java.util.*;
import java.util.stream.IntStream;

/**
 * Created by dl85 on 2016/3/24.
 */
public class TestDirectMain {


    public static void main(String[] args) {

        Map params = new HashMap();
        params.put("key1", "value1");
        params.put("key2", "value2");

        Thread.UncaughtExceptionHandler h = new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                e.printStackTrace();
            }
        };
        String tag= "test";

        try {
            System.out.println("~~~START~~~");

            DataQueueCenter dqc = new DataQueueCenter();
            //create the direct grouping of the queue
            dqc.createDirectQueue(tag, 4, null);

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
            dataFlush.setHandler(h);
            //start multi threads to process
            dataFlush.start();

            //create container
            List<List> store = new ArrayList();
            IntStream.range(0,dataFlush.getThreadNumber()).forEach(v->store.add(new ArrayList()));
            //add test data
            for (int i = 0; i <= 10; i++) {
                //just mode 3 , not 4, so the fourth thread won't process data
                Integer value = new Integer(i % 3);
                store.get(value.hashCode() % dataFlush.getThreadNumber()).add(value);
            }
            //add data to queue
            for (int i = 0; i < store.size(); i++) {
                dqc.addDataDirect(tag, store.get(i).toArray(), i);
            }

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
