package io.openmessaging.dao.storage;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;

/**
 * Created by bson on 17-5-25.
 */
public class TopicNode extends PersisitNode {

    private String topicName;

    TopicNode(String topicName, StorageInfo storageInfo) {
        this(topicName, storageInfo, null);
    }

    TopicNode(String topicName, StorageInfo storageInfo, KeyValue properties) {
        super(topicName, storageInfo, properties);
    }

    boolean persistance(BytesMessage message) {
        try {
            super.persist(message);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    BytesMessage get() {
        try {
            return super.get();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
