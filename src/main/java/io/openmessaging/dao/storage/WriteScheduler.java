package io.openmessaging.dao.storage;

import io.openmessaging.BytesMessage;
import io.openmessaging.MessageHeader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by BSONG on 2017/6/1.
 */
public class WriteScheduler implements Runnable {

    private final LinkedBlockingQueue<BytesMessage> tasks = new LinkedBlockingQueue(10000);

    private List<TopicWriteWorker> topicWorkers;
    private List<QueueWriteWorker> queueWorkers;
    private volatile boolean shutDown;

    public WriteScheduler(int workNum) {
        topicWorkers = new ArrayList<>(workNum);
        queueWorkers = new ArrayList<>(workNum);
        for (int i = 0; i < workNum; i++) {
            topicWorkers.add(new TopicWriteWorker(i));
        }
        for (int i = 0; i < workNum; i++) {
            queueWorkers.add(new QueueWriteWorker(i));
        }
    }

    @Override
    public void run() {
        while (tasks.isEmpty()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        topicWorkers.forEach(worker -> new Thread(worker).start());
        queueWorkers.forEach(worker -> new Thread(worker).start());

        while (!shutDown) {
            try {
                BytesMessage message = tasks.poll(100, TimeUnit.MILLISECONDS);
                if (message == null) {
                    continue;
                }
                String topic = message.headers().getString(MessageHeader.TOPIC);
                String queue = message.headers().getString(MessageHeader.QUEUE);
                if (topic != null) {
                    topicWorkers.get(StorageEngine.getWriteReplicationIndex(topic, null)).addMessageJob(message);
                }
                if (queue != null) {
                    queueWorkers.get(StorageEngine.getWriteReplicationIndex(null, queue)).addMessageJob(message);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean addTask(BytesMessage message) {
        try {
            tasks.put(message);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean taskAllDone() {

        return tasks.isEmpty() ? topicWorkers.stream().map(worker -> worker.isJobDone()).filter(isDone -> !isDone).count() == 0 && queueWorkers.stream().map(worker -> worker.isJobDone()).filter(isDone -> !isDone).count() == 0 : false;
    }

    public boolean tryStop(){
        synchronized (this){
            while (!taskAllDone()){
                try {
                    this.wait(100);
                } catch (InterruptedException e) {}
            }
        }

        shutDown = true;
        topicWorkers.forEach(worker-> worker.tryShutDown());
        queueWorkers.forEach(worker-> worker.tryShutDown());
        return true;
    }
}
