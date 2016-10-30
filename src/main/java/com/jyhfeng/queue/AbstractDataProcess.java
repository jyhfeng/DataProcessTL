package com.jyhfeng.queue;

import com.jyhfeng.queue.exception.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Created by dl85 on 2015/2/25.
 */
abstract class AbstractDataProcess implements Runnable, ILock {
    protected Logger log = LogManager.getLogger(this.getClass());
    private DataQueueCenter dataQueueCenter;
    private List<Thread> threadGroup;
    private Thread currentThread;
    private Queue dataQueue;
    private ConcurrentLinkedQueue dataFailQueue;
    private String consumerClassName;
    private String tag;
    private Lock lock;
    private Condition lockCond;
    private Map params;
    private Thread.UncaughtExceptionHandler handler;

    /**
     * default thread number: available processors-1 or 1
     */
    private int threadNumber;
    /**
     * default: flush data size
     */
    private int flushDataNumber = 1000;
    /**
     * default: 10 seconds
     */
    private long flushTime = 10000;
    /**
     * default: 10 milli seconds (minimum)
     */
    private long sleepTime = 10L;
    /**
     * default: per 20 data sleep once (min:10 data)
     */
    private int releaseCpuControlLimit = 20;
    /**
     * default: wait task done ,group join sleep time
     */
    private int waitGroupJoinSleepTime = 10;
    /**
     * default:BATCH_TYPE
     */
    private ThreadType threadType = ThreadType.BATCH_TYPE;
    /**
     * is service stop
     */
    private boolean isStop;
    /**
     * is Daemon thread;
     */
    private boolean isDaemon = true;
    /**
     * break loop when exception occurs
     */
    private boolean breakWhenError = Boolean.TRUE;
    /**
     * is Add Fail data
     */
    private boolean isAddFail;
    /**
     * final flush count by every thread dead
     */
    private volatile AtomicInteger finalFlushCount;


    public void setAddFail(boolean isAddFail) {
        this.isAddFail = isAddFail;
    }

    public AbstractDataProcess() {
        this.setThreadNumber(-1);
    }

    /**
     * Constructor
     *
     * @param consumerClassName
     * @param tag
     * @throws Exception
     */
    public AbstractDataProcess(String consumerClassName, String tag) throws Exception {
        this(consumerClassName, tag, -1);
    }

    /**
     * Constructor
     *
     * @param consumerClassName
     * @param tag
     * @param threadNumber
     * @throws Exception
     */
    public AbstractDataProcess(String consumerClassName, String tag, int threadNumber) throws Exception {
        this.setConsumerClassName(consumerClassName);
        this.setTag(tag);
        this.setThreadNumber(threadNumber);
    }

    @Override
    public void run() {

        long startTime = System.currentTimeMillis();
        this.threadGroup = new ArrayList();
        this.dataQueue = new ConcurrentLinkedQueue();
        this.dataFailQueue = new ConcurrentLinkedQueue();
        Map systemParams = this.initSystemParams();
        try {
            //init
            this.init(params);
            //init thread
            for (int i = 0; i < threadNumber; i++) {
                Class c = Class.forName(consumerClassName);
                Object o = c.newInstance();

                //set params map
                this.setParameters(c, o, "setParams", Map.class, this.params);
                //set system params map
                this.setParameters(c, o, "setSystemParams", Map.class, systemParams);
                //set queue
                this.setParameters(c, o, "setDataFailQueue", Queue.class, this.dataFailQueue);
                //set lock interface
                this.setParameters(c, o, "setLock", ILock.class, this);
                //set sourceQueue
                this.setParameters(c, o, "setSourceQueue", BlockingQueue.class, this.dataQueueCenter.getDataQueue(tag, i));
                switch (threadType) {
                    case BATCH_TYPE:
                        //set queue
                        this.setParameters(c, o, "setDataQueue", Queue.class, this.dataQueue);
                        break;
                    default:
                        break;
                }
                Thread t = new Thread((Runnable) o);
                t.setDaemon(this.isDaemon);
                t.setName("ThreadPool-" + tag + "-" + (i + 1));
                t.start();
                threadGroup.add(t);
            }

            //process single/batch data
            switch (threadType) {
                case SINGLE_TYPE:
                    //do nothing
                    this.service(this.dataFailQueue, params);
                    break;
                case BATCH_TYPE:
                    this.batchService(this.dataQueue, this.dataFailQueue, params);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            log.error("process data queue have errors\n" + StringUtils.getStringFromStackTrace(e));
            throw new RuntimeException(e.fillInStackTrace());
        } finally {
            this.destroy(params);
        }

        log.debug("cost time:" + (System.currentTimeMillis() - startTime));
    }

    /**
     * wait the thread die,then finish the program
     *
     * @throws Exception
     */
    private void service(Queue dataFailQueue, Map params) throws Exception {
        while (true) {
            if (this.isStop) {
                this.flushFailEvent(dataFailQueue, params);
                break;
            }

            lock.lock();
            try {
                lockCond.await(this.flushTime, TimeUnit.MILLISECONDS);
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * process batch data. If you have other demand,you can override this method
     *
     * @throws Exception
     */
    private void batchService(Queue dataQueue, Queue dataFailQueue, Map params) throws Exception {

        long nowTime;
        while (true) {
            nowTime = System.currentTimeMillis();
            //program stop,flush it if have data
            if (this.isStop) {
                flushMethod(dataQueue, dataFailQueue, params);
                break;
            }

            this.lock.lock();
            try {
                this.lockCond.await(this.flushTime, TimeUnit.MILLISECONDS);
            } finally {
                this.lock.unlock();
            }

            //if data is reach the limit,flush it
            if (dataQueue.size() >= this.flushDataNumber) {
                flushMethod(dataQueue, dataFailQueue, params);
            } else if ((System.currentTimeMillis() - nowTime) >= this.flushTime && dataQueue.size() > 0) {
                //if data is not much enough,but wait too much time,flush it
                flushMethod(dataQueue, dataFailQueue, params);
            }
        }
    }

    /**
     * Override this.methos to do something with fail data
     *
     * @param dataFailQueue
     * @throws Exception
     */
    public abstract void flushFail(Queue dataFailQueue, Map params) throws Exception;

    /**
     * Override this method to do something with data
     * EX:flush to data base
     *
     * @throws Exception
     */
    public abstract void flush(Queue dataQueue, Map params) throws Exception;

    /**
     * send End Tag
     *
     * @throws Exception
     */
    public void sendEndTag() throws Exception {
        //Add End Tag let BlockQueue stop the thread
        this.dataQueueCenter.addEndData(this.tag, this.threadNumber);
    }

    /**
     * Wait Task done
     *
     * @throws Exception
     */
    public void waitTaskDone() throws Exception {
        //thread group join
        this.threadGroupJoin();
        //current thread join
        this.currentThread.join();
        //garbage collection
        this.recycle();
    }

    /**
     * make thread group join
     *
     * @throws Exception
     */
    private void threadGroupJoin() throws InterruptedException {
        //program runs too fast,cause threadGroup is null or group size is not enough
        while (null == this.threadGroup || threadGroup.size() != this.threadNumber) {
            Thread.sleep(this.waitGroupJoinSleepTime);
        }

        for (Thread t : this.threadGroup) {
            t.join();
        }
    }


    /**
     * start the thread pool
     *
     * @throws Exception
     */
    public void start() throws Exception {

        this.lock = new ReentrantLock();
        this.lockCond = this.lock.newCondition();

        Objects.requireNonNull(consumerClassName);
        if (consumerClassName.trim().isEmpty()) {
            throw new IllegalArgumentException("please input a consumer class name !");
        }
        Objects.requireNonNull(tag);
        if (tag.trim().isEmpty()) {
            throw new IllegalArgumentException("please input a tag name !");
        }

        Objects.requireNonNull(this.dataQueueCenter);
        if(this.dataQueueCenter.isContainQueue(tag)==Boolean.FALSE){
            throw new IllegalArgumentException("please create queue first !");
        }

        if (threadNumber <= 0) {
            int temp = Runtime.getRuntime().availableProcessors();
            this.threadNumber = temp > 1 ? temp - 1 : 1;
        }
        this.finalFlushCount = new AtomicInteger();
        //start the pool
        currentThread = new Thread(this);
        currentThread.setName("ThreadPoolManager-" + tag);
        //set Exception
        if (null == handler) {
            handler = (Thread t, Throwable e) -> {
                this.dataQueueCenter.forceStopThread(tag, threadNumber);
            };
        }
        currentThread.setUncaughtExceptionHandler(handler);
        currentThread.start();
    }


    /**
     * setter add argus
     *
     * @param c
     * @param o
     * @param methodName
     * @param paramType
     * @param param
     * @throws Exception
     */
    private void setParameters(Class c, Object o, String methodName, Class paramType, Object param) throws Exception {
        Method threadMethod = null;
        while (threadMethod == null) {
            if (c == AbstractConsumer.class) {
                threadMethod = c.getDeclaredMethod(methodName, paramType);
            } else {
                c = c.getSuperclass();
            }
        }
        threadMethod.setAccessible(true);
        threadMethod.invoke(o, param);
    }

    /**
     * notify the lock object
     */
    public void wakeup() {
        if (null != this.lock) {
            this.lock.lock();
            try {
                this.lockCond.signal();
            } finally {
                this.lock.unlock();
            }
        }
    }

    /**
     * when thread get END TAG,it will try final flush if the dead threads number are enough
     */
    public synchronized void finalFlush() {

        if (finalFlushCount.incrementAndGet() != this.getThreadNumber()) {
            return;
        }
        try {
            //check lock object is locked or not
            //forgot why the reason of if condition I added ,but It should be found a small bug at CustomerProfile Project
            if (Thread.holdsLock(this.lock)) {
                flushMethod(dataQueue, dataFailQueue, params);
            }
        } catch (Exception e) {
            log.error("final flush has error", StringUtils.getStringFromStackTrace(e));
        }
        this.closeJob();
        this.wakeup();
    }

    /**
     * remove the tag in DataQueueCenter
     */
    private void closeJob() {

        isStop = true;
        if (null != this.dataQueueCenter) {
            this.dataQueueCenter.removeData(tag, this.threadNumber);
        }
    }


    /**
     * init
     */
    public abstract void init(Map params);

    /**
     * destroy
     */
    public abstract void destroy(Map params);

    /**
     * garbage collection
     */
    private void recycle() {
        if (null != this.dataQueue) {
            this.dataQueue.clear();
            this.dataQueue = null;
        }
        if (null != this.dataFailQueue) {
            this.dataFailQueue.clear();
            this.dataFailQueue = null;
        }
        if (null != dataQueueCenter) {
            dataQueueCenter.destroy();
            dataQueueCenter = null;
        }
        if (null != lockCond) {
            lockCond = null;
        }

        if (null != lock) {
            lock = null;
        }

        if (null != this.threadGroup) {
            this.threadGroup.clear();
            this.threadGroup = null;
        }

        if (null != params) {
            params.clear();
            params = null;
        }

    }

    /**
     * flush data & fail data
     *
     * @param dataQueue
     * @param dataFailQueue
     * @param params
     * @throws Exception
     */
    private void flushMethod(Queue dataQueue, Queue dataFailQueue, Map params) throws Exception {
        this.flushEvent(dataQueue, params);
        this.flushFailEvent(dataFailQueue, params);
    }

    /**
     * init system params
     *
     * @return
     */
    private Map initSystemParams() {
        Map tmp = new HashMap<>();
        tmp.put(Constants.TAG, this.tag);
        tmp.put(Constants.SLEEP_TIME, this.sleepTime);
        tmp.put(Constants.RELEASE_CPU_CONTROL_LIMIT, this.releaseCpuControlLimit);
        tmp.put(Constants.IS_ADD_FAIL, this.isAddFail);
        tmp.put(Constants.BREAK_WHEN_ERROR_OCCUR, this.breakWhenError);
        return tmp;
    }

    /**
     * flush fail event
     *
     * @param dataFailQueue
     * @throws Exception
     */
    private void flushFailEvent(Queue dataFailQueue, Map params) throws Exception {
        if (isAddFail && dataFailQueue.size() > 0) {
            this.flushFail(dataFailQueue, params);
        }
    }

    /**
     * flush event
     *
     * @param dataQueue
     * @throws Exception
     */
    private void flushEvent(Queue dataQueue, Map params) throws Exception {
        if (dataQueue.size() > 0) {
            this.flush(dataQueue, params);
        }
    }


    public void setConsumerClassName(String consumerClassName) {
        this.consumerClassName = consumerClassName;
    }


    public void setTag(String tag) {
        this.tag = tag;
    }


    public void setFlushDataNumber(int flushDataNumber) {
        this.flushDataNumber = flushDataNumber;
    }


    public void setFlushTime(long flushTime) {
        this.flushTime = flushTime;
    }

    public int getThreadNumber() {
        return threadNumber;
    }

    public void setThreadNumber(int threadNumber) {
        this.threadNumber = threadNumber;
    }

    public void setDaemon(boolean daemon) {
        isDaemon = daemon;
    }

    public void setReleaseCpuControlLimit(int releaseCpuControlLimit) {
        if (releaseCpuControlLimit < 10) {
            return;
        }
        this.releaseCpuControlLimit = releaseCpuControlLimit;
    }


    public void setParams(Map params) {
        this.params = params;
    }

    public void setSleepTime(long sleepTime) {
        if (sleepTime <= 0) {
            return;
        }
        this.sleepTime = sleepTime;
    }

    public boolean isBreakWhenError() {
        return breakWhenError;
    }

    public void setBreakWhenError(boolean breakWhenError) {
        this.breakWhenError = breakWhenError;
    }

    public void setHandler(Thread.UncaughtExceptionHandler handler) {
        this.handler = handler;
    }

    public void setThreadType(ThreadType threadType) {
        this.threadType = threadType;
    }


    public int getWaitGroupJoinSleepTime() {
        return waitGroupJoinSleepTime;
    }

    public void setWaitGroupJoinSleepTime(int waitGroupJoinSleepTime) {
        this.waitGroupJoinSleepTime = waitGroupJoinSleepTime;
    }

    public DataQueueCenter getDataQueueCenter() {
        return dataQueueCenter;
    }

    public void setDataQueueCenter(DataQueueCenter dataQueueCenter) {
        this.dataQueueCenter = dataQueueCenter;
    }
}
