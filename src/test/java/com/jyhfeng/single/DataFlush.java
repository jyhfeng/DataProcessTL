package com.jyhfeng.single;

import com.jyhfeng.queue.DataProcess;

import java.util.Map;
import java.util.Queue;

/**
 * Created by dl85 on 2015/3/16.
 */
public class DataFlush extends DataProcess {
    @Override
    public void flushFail(Queue dataFailQueue, Map params) throws Exception {
        System.out.println("FLUSH FAIL DATA:" + dataFailQueue.size());
    }

    @Override
    public void flush(Queue dataQueue, Map params) throws Exception {

    }
}
