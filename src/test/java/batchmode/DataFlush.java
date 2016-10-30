package batchmode;

import com.jyhfeng.queue.DataProcess;

import java.util.Map;
import java.util.Queue;


/**
 * Created by dl85 on 2015/3/4.
 */
public class DataFlush extends DataProcess {

    long count = 0;

    @Override
    public void flush(Queue dataQueue, Map params) throws Exception {
        int nowSize = dataQueue.size();
        //count += nowSize;
        for (int i = 0; i < nowSize; i++) {
            count += (int) dataQueue.poll();
        }

        System.out.println("current data sum:" + count);
    }

    @Override
    public void flushFail(Queue dataFailQueue, Map params) throws Exception {
        System.out.println("FLUSH FAIL DATA:" + dataFailQueue.size());
    }

    @Override
    public void destroy(Map params) {
        System.out.println("Finally count:"+count);
    }
}
