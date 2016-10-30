package com.jyhfeng.single;

import com.jyhfeng.queue.DataConsumer;

import java.util.Map;

/**
 * Created by dl85 on 2015/3/4.
 */
public class SingleDataConsumer extends DataConsumer {
    @Override
    public void service(Object obj, Map params) throws Exception {
        System.out.println(Thread.currentThread().getName() + "  obj value:" + obj.toString() + "  ");
    }

}
