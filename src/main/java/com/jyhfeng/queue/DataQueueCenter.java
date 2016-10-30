package com.jyhfeng.queue;

import com.jyhfeng.queue.exception.ThreadClosedException;
import com.jyhfeng.queue.exception.NoSuckKeyException;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.IntStream;


/**
 * Created by dl85 on 2015/2/25.
 */
public class DataQueueCenter {

    protected static final String END_TAG = "!@#END#@!##^&**f*s?%@!_55S2Rc";
    private ConcurrentHashMap<String, BlockingQueue> storeData;


    /**
     * Constructs
     */
    public DataQueueCenter() {
        storeData = new ConcurrentHashMap<String, BlockingQueue>();
    }


    /**
     * Add Data to Queue
     *
     * @param key
     * @param datas
     */
    public void addData(String key, Object[] datas) throws Exception {
        Objects.requireNonNull(key);
        if (key.trim().isEmpty()) {
            throw new IllegalArgumentException("please input a key");
        }

        if (null != datas) {

            if (storeData.containsKey(key) == Boolean.FALSE) {
                throw new ThreadClosedException("Thread is closing now,Please create queue first.(call createQueue())");
            } else {
                BlockingQueue queue = storeData.get(key);
                queue.addAll(Arrays.asList(datas));
            }
        }
    }

    /**
     * Add Data to Queue
     *
     * @param key
     * @param datas
     */
    public void addDataDirect(String key, Object[] datas, int taskId) throws Exception {
        Objects.requireNonNull(key);
        if (key.trim().isEmpty()) {
            throw new IllegalArgumentException("please input a key");
        }

        if (null != datas) {
            String k = this.getKey(key, taskId);
            if (null == k || this.storeData.containsKey(k) == Boolean.FALSE) {
                throw new ThreadClosedException("Thread is closing now,Please create queue first.(call createQueue())");
            }

            BlockingQueue queue = storeData.get(k);
            queue.addAll(Arrays.asList(datas));
        }
    }

    /**
     * Get Data Queue
     *
     * @param key
     * @return
     * @throws Exception
     */
    public synchronized BlockingQueue getDataQueue(String key, Integer taskId) throws Exception {
        String combineKey = key + "_" + taskId;
        if (this.storeData.containsKey(combineKey)) {
            return this.storeData.get(combineKey);
        }
        if (storeData.containsKey(key)) {
            return this.storeData.get(key);
        }

        throw new NoSuckKeyException("No Key(queue) Exist in Map " + combineKey + "/" + key + ",please create queue before thread start");
    }

    /**
     * is contain the queue
     * @param key
     * @return
     */
    public boolean isContainQueue(String key) {
        String combineKey = key + "_0";
        if (this.storeData.containsKey(combineKey)) {
            return Boolean.TRUE;
        }
        if (this.storeData.containsKey(key)) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    /**
     * garbage collection
     */
    public void destroy() {
        storeData.clear();
        storeData = null;

    }


    /**
     * add end data - let blocking queue to break the loop
     *
     * @param key
     * @param threadNum
     */
    protected void addEndData(String key, int threadNum) {
        if (null != this.storeData) {
            IntStream.range(0, threadNum).forEach(i -> {
                String rightKey = getKey(key, i);
                if (Objects.nonNull(rightKey)) {
                    this.storeData.get(rightKey).add(END_TAG.toString());
                }
            });
        }
    }

    /**
     * remove the data
     *
     * @param key
     */
    protected void removeData(String key, int threadNumber) {
        if (null != this.storeData) {
            IntStream.range(0, threadNumber).forEach(i -> {
                String rightKey = this.getKey(key, i);
                if (Objects.nonNull(rightKey)) {
                    this.storeData.remove(rightKey);
                }
            });
        }
    }


    /**
     * create empty queue
     *
     * @param key
     * @param queueSize
     */
    public void createQueue(String key, Integer queueSize) {
        this.storeData.put(key, null == queueSize ? new LinkedBlockingQueue() : new LinkedBlockingQueue(queueSize));
    }

    /**
     * create empty queue
     *
     * @param key
     * @param size
     * @param queueSize
     */
    public void createDirectQueue(String key, int size, Integer queueSize) {
        IntStream.range(0, size).forEach(i -> {
            if (this.storeData.contains(key + "_" + i)) {
                return;
            } else {
                this.storeData.put(key + "_" + i, null == queueSize ? new LinkedBlockingQueue() : new LinkedBlockingQueue(queueSize));
            }
        });
    }

    public void forceStopThread(String key, int threadNum) {
        if (null != storeData) {
            IntStream.range(0, threadNum).forEach(i -> {
                String rightKey = this.getKey(key, i);
                if (Objects.nonNull(rightKey)) {
                    storeData.get(rightKey).clear();
                }
            });
            this.addEndData(key, threadNum);
        }
    }

    public String printDataQueueInfo() {
        StringBuilder builder = new StringBuilder();
        builder.append("Queue Name/ Queue Size:\n");
        if (Objects.nonNull(this.storeData)) {
            this.storeData.forEach((key, value) -> builder.append(key).append(":").append(value.size()).append(","));
        }
        builder.append("\nProhibit key:\n");

        return builder.toString();
    }

    public int getQueueSize(String key) {
        if (this.storeData.containsKey(key)) {
            return this.storeData.get(key).size();
        }
        return -1;
    }

    /**
     * get match key
     *
     * @param key
     * @param taskId
     * @return
     */
    private String getKey(String key, Integer taskId) {
        String combineKey = key + "_" + taskId;
        if (this.storeData.containsKey(combineKey)) {
            return combineKey;
        }
        if (storeData.containsKey(key)) {
            return key;
        }
        return null;
    }

}
