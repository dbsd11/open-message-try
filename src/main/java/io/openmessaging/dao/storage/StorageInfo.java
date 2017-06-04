package io.openmessaging.dao.storage;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by bson on 17-6-1.
 */
class StorageInfo {
    private static String msgRootPath = "/home/admin/race2017/data";
    private String replicationName;
    private String nodeName;

    public StorageInfo(String replicationName) {
        this.replicationName = replicationName;
    }

    public String getReplicationName() {
        return replicationName;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public static void updateMsgRootPath(String msgRootPath, List<Replication> replications) {
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            for (Replication replication : replications) {
                String replicationPath = String.join("/", msgRootPath, replication.getName());
                File replicaiton = new File(replicationPath);
                if (replicaiton.exists()) {
//                    deleteDir(replicaiton);
                }
            }
        }));
        StorageInfo.msgRootPath = msgRootPath;
    }

    public String getNodePath() {
        if (!new File(msgRootPath).exists()) {
            msgRootPath = "/home/admin/test";
        }
        String nodePath = String.join("/", msgRootPath, replicationName, nodeName);
        File node = new File(nodePath);
        if (!node.exists()) {
            node.mkdirs();
        }
        return nodePath;
    }

    @Override
    public StorageInfo clone() {
        try {
            return (StorageInfo) super.clone();
        } catch (Exception e) {
            return new StorageInfo(replicationName);
        }
    }

    static void deleteDir(File file) {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                deleteDir(f);
            }
            file.delete();
        } else {
            file.delete();
        }
    }

    public static void main(String[] args){
        StorageInfo.updateMsgRootPath("E:\\JPrograms\\open-messaging-demo\\target\\classes", new LinkedList<Replication>(){{add(new Replication("1"));}});
    }
}
