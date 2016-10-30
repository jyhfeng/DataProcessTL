package batchmode;

import com.jyhfeng.queue.DataConsumer;

import java.util.Map;

/**
 * Created by dl85 on 2015/2/25.
 */
public class BatchDataConsumer extends DataConsumer {

    long numCount = 0;
    long spend;

    @Override
    public void service(Object obj, Map params) throws Exception {
        this.getDataQueue().add(obj);
        numCount++;
    }

    @Override
    public void init() {
        this.spend = System.currentTimeMillis();
    }

    @Override
    public void destroy() {
        System.out.println(Thread.currentThread().getName() + ",process count:" + numCount + ",cost time:" + (System.currentTimeMillis() - spend) + "ms");
    }

}

