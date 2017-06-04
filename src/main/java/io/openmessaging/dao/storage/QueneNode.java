package io.openmessaging.dao.storage;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;

/**
 * Created by bson on 17-5-25.
 */
public class QueneNode extends PersisitNode {

    private String queueName;


    QueneNode(String queueName, StorageInfo storageInfo) {
        this(queueName, storageInfo, null);
    }

    QueneNode(String queueName, StorageInfo storageInfo, KeyValue properties) {
        super(queueName, storageInfo, properties);
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
