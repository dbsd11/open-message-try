package io.openmessaging.dao.storage;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.demo.DefaultBytesMessage;
import me.doubledutch.lazyjson.LazyObject;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by BSONG on 2017/5/29.
 */
public abstract class PersisitNode {
    private String nodeName;

    private StorageInfo storageInfo;
    private KeyValue properties;


    private MsgFileObj readResource;
    private MsgFileObj writeresource;

    PersisitNode(String nodeName, StorageInfo storageInfo) {
        this(nodeName, storageInfo, null);
    }

    PersisitNode(String nodeName, StorageInfo storageInfo, KeyValue properties) {
        this.nodeName = nodeName;
        storageInfo.setNodeName(nodeName);
        this.storageInfo = storageInfo;
        this.properties = properties;
    }

    boolean persist(BytesMessage message) throws IOException {
        if (this.writeresource == null) {
            this.writeresource = new MsgFileObj(storageInfo, "w", properties);
        }
        String body = new String(message.getBody());
        LazyObject header = new LazyObject();
        LazyObject prop = new LazyObject();
        message.headers().keySet().forEach(key -> {
            header.put(key, message.headers().getString(key));
        });
        if (message.properties() != null) {
            message.properties().keySet().forEach(key -> {
                prop.put(key, message.properties().getString(key));
            });
        }
        LazyObject mapObj = new LazyObject();
        mapObj.put("body", body);
        mapObj.put("header", header);
        mapObj.put("prop", prop);
        return writeresource.write(mapObj.toString().getBytes());
    }

    BytesMessage get() throws IOException {
        if (this.readResource == null) {
            this.readResource = new MsgFileObj(storageInfo, "r", properties);
        }
        byte[] data = readResource.preRead();
        if (data == null) {
            return null;
        }
        String dataStr = new String(data).trim();
        if (dataStr.length() == 0) {
            return null;
        }
        LazyObject mapObj = new LazyObject(dataStr);
        LazyObject headObj = mapObj.getJSONObject("header");
        LazyObject propObj = mapObj.getJSONObject("prop");

        BytesMessage message = new DefaultBytesMessage(mapObj.getString("body").getBytes());
        headObj.keySet().forEach(key -> {
            Object value = headObj.getString(key);
            if (value instanceof Integer) {
                message.putHeaders(key, (Integer) value);
            }
            if (value instanceof Long) {
                message.putHeaders(key, (Long) value);
            }
            if (value instanceof Double) {
                message.putHeaders(key, (Double) value);
            }
            if (value instanceof String) {
                message.putHeaders(key, (String) value);
            }
        });
        propObj.keySet().forEach(key -> {
            Object value = propObj.getString(key);
            if (value instanceof Integer) {
                message.putProperties(key, (Integer) value);
            }
            if (value instanceof Long) {
                message.putProperties(key, (Long) value);
            }
            if (value instanceof Double) {
                message.putProperties(key, (Double) value);
            }
            if (value instanceof String) {
                message.putProperties(key, (String) value);
            }
        });

        return message;
    }

    static class MsgFileObj {
        private static int pageSize = 100000;

        private volatile int readPage;
        private volatile int readIndex;
        private volatile long readOffset;
        private volatile FileChannel readChannel;
        private volatile LinkedBlockingQueue<String> pageCache;
        private int writePage;
        private int writeIndex;
        private BufferedOutputStream bos;
        private String mode;
        private String nodePath;
        private StorageInfo storageInfo;

        MsgFileObj(StorageInfo storageInfo, String mode, KeyValue properties) {
            this.readPage = properties.getInt("readPage");
            this.writePage = properties.getInt("writePage");
            this.readIndex = properties.getInt("readIndex");
            this.writeIndex = properties.getInt("writeIndex");
            this.readOffset = properties.getInt("readOffset");
            this.nodePath = storageInfo.getNodePath();
            this.mode = mode;
            this.storageInfo = storageInfo;

            allocatedReadWriteChannel();

            if ("r".equals(mode)) {
                pageCache = new LinkedBlockingQueue(pageSize);
                new Thread(() -> {
                    int i = 0;
                    while (i < 10) {
                        try {
                            byte[] data = read();
                            if (data != null) {
                                i = 0;
                                pageCache.put(new String(data));
                                continue;
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        i++;
                    }
                }).start();
            }
        }

        Boolean write(byte[] msg) throws IOException {
            try {
                byte[] lenBt = new byte[3];
                lenBt[2] = (byte) (msg.length & 0xff);
                lenBt[1] = (byte) (msg.length >> 8 & 0xff);
                lenBt[0] = (byte) (msg.length >> 16 & 0xff);
                bos.write(lenBt);
                bos.write(msg);
            } catch (Exception e) {
                if (e instanceof ClosedChannelException) {
                    allocatedReadWriteChannel();
                    return write(msg);
                }
                throw e;
            }

            if (++writeIndex == pageSize) {
                ++writePage;
                writeIndex = 0;
                allocatedReadWriteChannel();
            }
            return true;
        }

        byte[] preRead() throws IOException {
            try {
                String str = pageCache.take();
                if (str != null) {
                    return str.getBytes();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return read();
        }

        byte[] read() throws IOException {
            ByteBuffer lengBuff = ByteBuffer.allocate(3);
            try {
                readChannel.read(lengBuff, readOffset);
                readOffset += 3;
            } catch (Exception e) {
                if (e instanceof ClosedChannelException) {
                    allocatedReadWriteChannel();
                    return read();
                }
                throw e;
            }

            lengBuff.flip();
            if (!lengBuff.hasRemaining()) {
                return null;
            }

            int len = ((lengBuff.get() & 0xff) << 16) | ((lengBuff.get() & 0xff) << 8) | (lengBuff.get() & 0xff);
            ByteBuffer bb = ByteBuffer.allocate(len);
            try {
                readChannel.read(bb, readOffset);
                readOffset += len;
            } catch (Exception e) {
                readOffset -= 3;
                if (e instanceof ClosedChannelException) {
                    allocatedReadWriteChannel();
                    return read();
                }
                throw e;
            }

            if (++readIndex == pageSize) {
                ++readPage;
                readIndex = 0;
                readOffset = 0;
                allocatedReadWriteChannel();
            }
            bb.flip();
            return bb.array();
        }

        void allocatedReadWriteChannel() {
            try {
                if ("r".equals(mode)) {
                    if (readChannel != null) {
                        try {
                            readChannel.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    RandomAccessFile readFile = new RandomAccessFile(String.join("/", nodePath, String.valueOf(readPage)), "rw");
                    readChannel = readFile.getChannel();
                }
                if ("w".equals(mode)) {
                    if (bos != null) {
                        try {
                            bos.flush();
                            bos.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    File file = new File(String.join("/", nodePath, String.valueOf(writePage)));
                    file.createNewFile();
                    bos = new BufferedOutputStream(new FileOutputStream(file));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
