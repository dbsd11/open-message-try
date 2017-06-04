package io.openmessaging;

import io.openmessaging.exception.OMSResourceNotExistException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by BSONG on 2017/5/24.
 */
public class ResourceManagerServer implements ResourceManager {
    private static Set<String> consumerIds = new HashSet<>();
    private static Map<String, List<String>> nsNameMap = new ConcurrentHashMap<>();
    private static Map<String, List<String>> topicNameMap = new ConcurrentHashMap<>();
    private static Map<String, List<String>> queueNameMap = new ConcurrentHashMap<>();

    private static Map<String, List<KeyValue>> nsPropMap = new ConcurrentHashMap<>();
    private static Map<String, List<KeyValue>> topicPropMap = new ConcurrentHashMap<>();
    private static Map<String, List<KeyValue>> queuePropMap = new ConcurrentHashMap<>();

    private String consumerId;

    private ResourceManagerServer(String consumerId) {
        this.consumerId = consumerId;
    }

    public static ResourceManagerServer getInstance(String consumerId) {
        consumerIds.add(consumerId);
        if (!nsNameMap.containsKey(consumerId)) {
            nsNameMap.put(consumerId, new ArrayList<>());
        }
        if (!topicNameMap.containsKey(consumerId)) {
            topicNameMap.put(consumerId, new ArrayList<>());
        }
        if (!queueNameMap.containsKey(consumerId)) {
            queueNameMap.put(consumerId, new ArrayList<>());
        }
        if (!nsPropMap.containsKey(consumerId)) {
            nsPropMap.put(consumerId, new ArrayList<>());
        }
        if (!topicPropMap.containsKey(consumerId)) {
            topicPropMap.put(consumerId, new ArrayList<>());
        }
        if (!queuePropMap.containsKey(consumerId)) {
            queuePropMap.put(consumerId, new ArrayList<>());
        }
        return new ResourceManagerServer(consumerId);
    }

    @Override
    public void createAndUpdateNamespace(String nsName, KeyValue properties) {
        List<String> nsNames = nsNameMap.get(consumerId);
        int i = nsNames.indexOf(nsName);
        if (i == -1) {
            nsNames.add(nsName);
            nsPropMap.get(consumerId).add(properties);
        } else {
            nsPropMap.get(consumerId).set(i, properties);
        }
    }

    @Override
    public void createAndUpdateTopic(String topicName, KeyValue properties) {
        List<String> topicNames = topicNameMap.get(consumerId);
        int i = topicNames.indexOf(topicName);
        if (i == -1) {
            topicNames.add(topicName);
            topicPropMap.get(consumerId).add(properties);
        } else {
            topicPropMap.get(consumerId).set(i, properties);
        }
    }

    @Override
    public void createAndUpdateQueue(String queueName, Filters filter, KeyValue properties) {
        List<String> queueNames = queueNameMap.get(consumerId);
        int i = queueNames.indexOf(queueName);
        if (i == -1) {
            queueNames.add(queueName);
            queuePropMap.get(consumerId).add(properties);
        } else {
            queuePropMap.get(consumerId).set(i, properties);
        }
    }

    @Override
    public void destroyNamespace(String nsName) {
        List<String> nsNames = nsNameMap.get(consumerId);
        int i = nsNames.indexOf(nsName);
        if (i != -1) {
            nsNameMap.get(consumerId).remove(i);
            nsPropMap.get(consumerId).remove(i);
        }
    }

    @Override
    public void destroyTopic(String topicName) {
        List<String> topicNames = topicNameMap.get(consumerId);
        int i = topicNames.indexOf(topicName);
        if (i != -1) {
            topicNameMap.get(consumerId).remove(i);
            topicPropMap.get(consumerId).remove(i);
        }
    }

    @Override
    public void destroyQueue(String queueName) {
        List<String> queueNames = queueNameMap.get(consumerId);
        int i = queueNames.indexOf(queueName);
        if (i != -1) {
            queueNameMap.get(consumerId).remove(i);
            queuePropMap.get(consumerId).remove(i);
        }
    }

    @Override
    public KeyValue getNamespaceProperties(String nsName) throws OMSResourceNotExistException {
        List<String> nsNames = nsNameMap.get(consumerId);
        int i = nsNames.indexOf(nsName);
        if (i != -1) {
            return nsPropMap.get(consumerId).get(i);
        }
        return null;
    }

    @Override
    public KeyValue getTopicProperties(String topicName) throws OMSResourceNotExistException {
        List<String> topicNames = topicNameMap.get(consumerId);
        int i = topicNames.indexOf(topicName);
        if (i != -1) {
            return topicPropMap.get(consumerId).get(i);
        }
        return null;
    }

    @Override
    public KeyValue getQueueProperties(String queueName) throws OMSResourceNotExistException {
        List<String> queueNames = queueNameMap.get(consumerId);
        int i = queueNames.indexOf(queueName);
        if (i != -1) {
            return queuePropMap.get(consumerId).get(i);
        }
        return null;
    }

    @Override
    public List<String> consumerIdListInQueue(String queueName) throws OMSResourceNotExistException {
        return null;
    }

    @Override
    public KeyValue getConsumerProperties(String consumerId) throws OMSResourceNotExistException {
        return null;
    }

    @Override
    public void setConsumerProperties(String consumerId, KeyValue properties) throws OMSResourceNotExistException {

    }

    @Override
    public List<String> producerIdListInQueue(String queueName) throws OMSResourceNotExistException {
        return null;
    }

    @Override
    public List<String> producerIdListInTopic(String topicName) throws OMSResourceNotExistException {
        return null;
    }

    @Override
    public KeyValue getProducerProperties(String producerId) throws OMSResourceNotExistException {
        return null;
    }

    @Override
    public void setProducerProperties(String producerId, KeyValue properties) throws OMSResourceNotExistException {

    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }
}
