package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import io.openmessaging.dao.storage.StorageEngine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class DefaultPullConsumer implements PullConsumer {
    private KeyValue properties;
    private String queue;
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();
    private int lastIndex = 0;

    private String customerId = UUID.randomUUID().toString();

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        if (properties != null && properties.containsKey("STORE_PATH")) {
            StorageEngine.updateMsgRootPath(properties.getString("STORE_PATH"));
        }
    }


    @Override
    public KeyValue properties() {
        return properties;
    }


    @Override
    public synchronized Message poll() {
        if (buckets.size() == 0 || queue == null) {
            return null;
        }

        for (int i = 0; i < bucketList.size(); i++) {
            Message message = null;
            String bucket = bucketList.get(lastIndex++ % bucketList.size());
            if (queue.equals(bucket)) {
                message = StorageEngine.tryGet(null, null, queue);
            } else {
                message = StorageEngine.tryGet(customerId, bucket, queue);
            }
            if (message != null) {
                return message;
            }
        }
        return null;
    }

    @Override
    public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public synchronized void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        buckets.add(queueName);
        buckets.addAll(topics);
        bucketList.clear();
        bucketList.addAll(buckets);
    }


}
