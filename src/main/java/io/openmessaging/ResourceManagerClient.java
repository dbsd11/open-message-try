package io.openmessaging;

import io.openmessaging.exception.OMSResourceNotExistException;

import java.util.List;

/**
 * Created by BSONG on 2017/5/24.
 */
public class ResourceManagerClient implements ResourceManager {

    private ResourceManagerServer resourceManagerServer = null;

    private String consumerId;

    public ResourceManagerClient(String consumerId){
        this.consumerId = consumerId;
    }

    @Override
    public void createAndUpdateNamespace(String nsName, KeyValue properties) {
        checkNotShutDown();
        resourceManagerServer.createAndUpdateNamespace(nsName, properties);
    }

    @Override
    public void createAndUpdateTopic(String topicName, KeyValue properties) {
        checkNotShutDown();
        resourceManagerServer.createAndUpdateTopic(topicName, properties);
    }

    @Override
    public void createAndUpdateQueue(String queueName, Filters filter, KeyValue properties) {
        checkNotShutDown();
        resourceManagerServer.createAndUpdateQueue(queueName, filter, properties);
    }

    @Override
    public void destroyNamespace(String nsName) {
        checkNotShutDown();
        resourceManagerServer.destroyNamespace(nsName);
    }

    @Override
    public void destroyTopic(String topicName) {
        checkNotShutDown();
        resourceManagerServer.destroyTopic(topicName);
    }

    @Override
    public void destroyQueue(String queueName) {
        checkNotShutDown();
        resourceManagerServer.destroyQueue(queueName);
    }

    @Override
    public KeyValue getNamespaceProperties(String nsName) throws OMSResourceNotExistException {
        checkNotShutDown();
        return resourceManagerServer.getNamespaceProperties(nsName);
    }

    @Override
    public KeyValue getTopicProperties(String topicName) throws OMSResourceNotExistException {
        checkNotShutDown();
        return resourceManagerServer.getTopicProperties(topicName);
    }

    @Override
    public KeyValue getQueueProperties(String queueName) throws OMSResourceNotExistException {
        checkNotShutDown();
        return resourceManagerServer.getQueueProperties(queueName);
    }

    @Override
    public List<String> consumerIdListInQueue(String queueName) throws OMSResourceNotExistException {
        checkNotShutDown();
        return resourceManagerServer.consumerIdListInQueue(queueName);
    }

    @Override
    public KeyValue getConsumerProperties(String consumerId) throws OMSResourceNotExistException {
        checkNotShutDown();
        return resourceManagerServer.getConsumerProperties(consumerId);
    }

    @Override
    public void setConsumerProperties(String consumerId, KeyValue properties) throws OMSResourceNotExistException {
        checkNotShutDown();
        resourceManagerServer.setConsumerProperties(consumerId, properties);
    }

    @Override
    public List<String> producerIdListInQueue(String queueName) throws OMSResourceNotExistException {
        checkNotShutDown();
        return resourceManagerServer.producerIdListInQueue(queueName);
    }

    @Override
    public List<String> producerIdListInTopic(String topicName) throws OMSResourceNotExistException {
        checkNotShutDown();
        return resourceManagerServer.producerIdListInTopic(topicName);
    }

    @Override
    public KeyValue getProducerProperties(String producerId) throws OMSResourceNotExistException {
        checkNotShutDown();
        return resourceManagerServer.getProducerProperties(producerId);
    }

    @Override
    public void setProducerProperties(String producerId, KeyValue properties) throws OMSResourceNotExistException {
        checkNotShutDown();
        resourceManagerServer.setProducerProperties(producerId, properties);
    }

    @Override
    public void start() {
        resourceManagerServer = ResourceManagerServer.getInstance(consumerId);
    }

    @Override
    public void shutdown() {
        synchronized (this){
            resourceManagerServer = null;
        }
    }

    public synchronized boolean checkNotShutDown(){
        if(resourceManagerServer == null){
            throw new RuntimeException("resource manager has been shutdown");
        }
        return true;
    }
}
