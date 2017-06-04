package io.openmessaging.dao.storage;

import io.openmessaging.BytesMessage;
import io.openmessaging.MessageHeader;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Created by BSONG on 2017/6/1.
 */
class QueueWriteWorker implements Runnable {

    private int id;

    private LinkedBlockingDeque<BytesMessage> messageQueue = new LinkedBlockingDeque();

    private volatile boolean shutDown;

    public QueueWriteWorker(int workId) {
        this.id = workId;
    }

    @Override
    public void run() {
        while (!shutDown) {
            try {
                BytesMessage message = messageQueue.take();
                if (message == null) {
                    continue;
                }
                String queue = message.headers().getString(MessageHeader.QUEUE);
                if (queue != null) {
                    StorageEngine.getReplication(id).queuePersist(queue, message);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void addMessageJob(BytesMessage message) throws InterruptedException {
        messageQueue.put(message);
    }

    public boolean isJobDone() {
        return messageQueue.isEmpty() ? true : false;
    }

    public void tryShutDown() {
        shutDown = true;
    }
}
