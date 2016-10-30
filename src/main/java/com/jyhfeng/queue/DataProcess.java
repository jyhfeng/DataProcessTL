package com.jyhfeng.queue;

import java.util.Map;
import java.util.Queue;

/**
 * Created by dl85 on 2015/7/20.
 */
 public abstract class DataProcess extends AbstractDataProcess{

    @Override
    public void init(Map params) {

    }

    @Override
    public void destroy(Map params) {

    }

    @Override
    public void flushFail(Queue dataFailQueue, Map params) throws Exception {

    }
}
