package com.jyhfeng.queue;

/**
 * Created by dl85 on 2015/3/4.
 */
public interface ILock {
    void wakeup();

    void finalFlush();
}
