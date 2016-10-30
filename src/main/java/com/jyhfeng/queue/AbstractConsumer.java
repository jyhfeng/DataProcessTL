package com.jyhfeng.queue;

import com.jyhfeng.queue.exception.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;


/**
 * Created by dl85 on 2015/2/25.
 */
abstract class AbstractConsumer implements Runnable {
    private Logger log = LogManager.getLogger(this.getClass());
    private Queue dataQueue;
    private Queue dataFailQueue;
    private ILock lock;
    private Map systemParams;
    private Map params;
    private BlockingQueue sourceQueue;

    public BlockingQueue getSourceQueue() {
        return sourceQueue;
    }

    public void setSourceQueue(BlockingQueue sourceQueue) {
        this.sourceQueue = sourceQueue;
    }

    public Queue getDataFailQueue() {
        return dataFailQueue;
    }

    void setDataFailQueue(Queue dataFailQueue) {
        this.dataFailQueue = dataFailQueue;
    }

    void setParams(Map params) {
        this.params = params;
    }

    void setSystemParams(Map systemParams) {
        this.systemParams = systemParams;
    }

    void setLock(ILock lock) {
        this.lock = lock;
    }

    public Queue getDataQueue() {
        return dataQueue;
    }

    void setDataQueue(Queue dataQueue) {
        this.dataQueue = dataQueue;
    }

    @Override
    public void run() {

        int releaseCpuControlLimit = Integer.parseInt(this.systemParams.get(Constants.RELEASE_CPU_CONTROL_LIMIT).toString());
        long sleepTime = Long.parseLong(this.systemParams.get(Constants.SLEEP_TIME).toString());
        boolean isAddFail = Boolean.valueOf(this.systemParams.get(Constants.IS_ADD_FAIL).toString());
        boolean breakWhenErrorOccur = Boolean.valueOf(this.systemParams.get(Constants.BREAK_WHEN_ERROR_OCCUR).toString());
        int count = 0;
        //init
        this.init();

        //process data
        Object obj = null;
        while (!Thread.currentThread().isInterrupted() && null != this.sourceQueue) {
            try {
                obj = this.sourceQueue.take();
                //check end tag
                if (obj instanceof String && obj.toString().equals(DataQueueCenter.END_TAG)) {
                    this.wakeLockObject();
                    break;
                }
                //process obj
                this.service(obj, this.params);
                //wake the lock up
                this.wakeLockObject();
                //release the cpu control,balance the thread workload
                if (count >= releaseCpuControlLimit) {
                    Thread.sleep(sleepTime);
                    count = 0;
                } else {
                    count++;
                }
            } catch (InterruptedException e) {
                log.error("have interrupted exception on consumer\n" + StringUtils.getStringFromStackTrace(e));
                this.addFailData(isAddFail, obj);
                if (breakWhenErrorOccur) {
                    break;
                }
            } catch (Exception e) {
                log.error("have error on consumer\n" + StringUtils.getStringFromStackTrace(e));
                this.addFailData(isAddFail, obj);
                if (breakWhenErrorOccur) {
                    break;
                }
            }
        }
        //destroy
        destroy();
        //final flush count
        this.lock.finalFlush();
    }

    /**
     * init -the start of life cycle
     *
     * @throws Exception
     */
    public abstract void init();

    /**
     * Process Data Queue
     *
     * @param obj
     * @param params
     * @throws Exception
     */
    public abstract void service(Object obj, Map params) throws Exception;

    /**
     * destroy - the end of life cycle
     */
    public abstract void destroy();

    /**
     * wake lock object
     */
    private void wakeLockObject() {
        if (null != this.lock) {
            this.lock.wakeup();
        }
    }

    /**
     * add Fail Data
     *
     * @param isAddFail
     * @param obj
     */
    private void addFailData(boolean isAddFail, Object obj) {
        if (isAddFail && null != obj) {
            this.dataFailQueue.add(obj);
        }
    }
}
