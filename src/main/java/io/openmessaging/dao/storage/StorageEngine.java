package io.openmessaging.dao.storage;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by BSONG on 2017/5/25.
 */
public class StorageEngine {
    private static List<Replication> replicates = new ArrayList<Replication>() {{
        add(new Replication("1"));
        add(new Replication("2"));
        add(new Replication("3"));
        add(new Replication("4"));
        add(new Replication("5"));
    }};

    private static Map<String, Integer> topicWriteMap = new ConcurrentHashMap<>();
    private static Map<String, Integer> queueWriteMap = new ConcurrentHashMap<>();
    private static Map<String, Integer> topicReadMap = new ConcurrentHashMap<>();
    private static Map<String, Integer> queueReadMap = new ConcurrentHashMap<>();

    private static WriteScheduler writeScheduler = new WriteScheduler(replicates.size());

    public static boolean persistant(String producerId, Message message) {
        if (message == null) {
            return false;
        }
        if (!(message instanceof BytesMessage)) {
            throw new UnsupportedOperationException("Unsupported message type, must[BytesMessage]");
        }
        if (((BytesMessage) message).getBody() == null) {
            return false;
        }

        writeScheduler.addTask((BytesMessage) message);
        return true;
    }

    //比较不合理
    public static BytesMessage tryGet(String custoemrtId, String topic, String queue) {
        if (topic != null) {
            String topicReadKey = topic + '_' + custoemrtId;
            return getReadReplication(topicReadKey, null).topicGet(topic, custoemrtId);
        }
        if (queue != null) {
            return getReadReplication(null, queue).queueGet(queue);
        }
        return null;
    }

    static int getWriteReplicationIndex(String topic, String queue) {
        if (topic != null) {
            if (!topicWriteMap.containsKey(topic)) {
                topicWriteMap.put(topic, 0);
            }
            int i = topicWriteMap.get(topic);
            topicWriteMap.put(topic, (i + 1) % replicates.size());
            return i;
        }
        if (queue != null) {
            if (!queueWriteMap.containsKey(queue)) {
                queueWriteMap.put(queue, 0);
            }
            int i = queueWriteMap.get(queue);
            queueWriteMap.put(queue, (i + 1) % replicates.size());
            return i;
        }
        return 0;
    }

    static Replication getReplication(int i) {

        return replicates.get(i);
    }

    static Replication getReadReplication(String topic, String queue) {
        if (topic != null) {
            if (!topicReadMap.containsKey(topic)) {
                topicReadMap.put(topic, 0);
            }
            int i = topicReadMap.get(topic);
            topicReadMap.put(topic, (i + 1) % replicates.size());
            return replicates.get(i);
        }
        if (queue != null) {
            if (!queueReadMap.containsKey(queue)) {
                queueReadMap.put(queue, 0);
            }
            int i = queueReadMap.get(queue);
            queueReadMap.put(queue, (i + 1) % replicates.size());
            return replicates.get(i);
        }
        return null;
    }

    public static void updateMsgRootPath(String msgRootPath) {

        StorageInfo.updateMsgRootPath(msgRootPath, replicates);
    }

    private static volatile int totalProducer = 0;
    private static volatile Set<String> flushProducers = new HashSet<>();

    public static synchronized void producerCreated(String producerId) {
        int count = totalProducer + 1;
        totalProducer = count;
    }

    public static void flush(String producerId) {
        flushProducers.add(producerId);
        if (flushProducers.size() == totalProducer) {
            writeScheduler.tryStop();
        }
    }

    static {
        new Thread(writeScheduler).start();
    }
}
