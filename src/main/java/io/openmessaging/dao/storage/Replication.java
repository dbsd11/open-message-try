package io.openmessaging.dao.storage;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.demo.DefaultKeyValue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by bson on 17-5-25.
 */
public class Replication {
    private String replicateName;

    private Map<String, TopicNode> topicMap;

    private Map<String, QueneNode> queueMap;

    private StorageInfo storageInfo;

    Replication(String replicateName) {
        this(replicateName, null);
    }

    Replication(String replicateName, KeyValue properties) {
        this.replicateName = replicateName;
        this.storageInfo = new StorageInfo(replicateName);
        this.topicMap = new ConcurrentHashMap<>();
        this.queueMap = new ConcurrentHashMap();
    }

    boolean addTopic(String topicName, KeyValue properties) {
        topicMap.put(topicName, new TopicNode(topicName, storageInfo.clone(), properties));
        return true;
    }

    boolean addReadTopic(String topicName, String customerId, KeyValue properties) {
        topicMap.put(topicName + '_' + customerId, new TopicNode(topicName, storageInfo.clone(), properties));
        return true;
    }

    boolean addQueue(String queueName, KeyValue propertie) {
        queueMap.put(queueName, new QueneNode(queueName, storageInfo.clone(), propertie));
        return true;
    }

    boolean topicPersist(String topicName, BytesMessage message) {
        if (!topicMap.containsKey(topicName)) {
            addTopic(topicName, new DefaultKeyValue());
        }
        return topicMap.get(topicName).persistance(message);
    }

    BytesMessage topicGet(String topicName, String custoemrtId) {
        if (!topicMap.containsKey(topicName)) {
            return null;
        }
        String topicReadKey = topicName + '_' + custoemrtId;
        if (!topicMap.containsKey(topicReadKey)) {
            addReadTopic(topicName, custoemrtId, new DefaultKeyValue());
        }
        return topicMap.get(topicReadKey).get();
    }

    boolean queuePersist(String queueName, BytesMessage message) {
        if (!queueMap.containsKey(queueName)) {
            addQueue(queueName, new DefaultKeyValue());
        }
        return queueMap.get(queueName).persistance(message);
    }

    BytesMessage queueGet(String queueName) {
        if (!queueMap.containsKey(queueName)) {
            addQueue(queueName, new DefaultKeyValue());
        }
        return queueMap.get(queueName).get();
    }

    public String getName() {
        return replicateName;
    }
}
