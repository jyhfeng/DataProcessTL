package batchmode;

import com.jyhfeng.queue.DataQueueCenter;
import com.jyhfeng.queue.ThreadType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Created by dl85 on 2015/2/25.
 */
public class TestBatchMain {

    public static void main(String[] args) {
        //create test data
        List store = new ArrayList();
        IntStream.range(0, 10000).forEach(v -> store.add(v));
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

        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("~~~END~~~");
    }

}
