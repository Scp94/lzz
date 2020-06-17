package com.lzz.zk2;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZkDistributedLock {
    private static final String HEAD = "/locks";
    private static final String HOST = "127.0.0.1:2181";
    private String parent;
    private String path;
    /**
     * 当前节点路径
     */
    private String currentPath;
    /**
     * 前一个节点路径
     */
    private String beforePath;
    private ZkClient zkClient;
    private CountDownLatch cdl = new CountDownLatch(1);

    /**
     * 指定在哪个持久节点下创建锁
     * @param path
     */
    public ZkDistributedLock(String path){
        zkClient = new ZkClient(HOST);
        this.parent = HEAD + "/" + path;
        this.path = path;
        if (!zkClient.exists(HEAD)) {
            try {
                zkClient.createPersistent(HEAD);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (!zkClient.exists(parent)) {
            try {
                zkClient.createPersistent(parent);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void lock() throws InterruptedException {
        if(tryLock()){
            System.out.println("获取锁成功");
//            Thread.sleep(100000);
        }else{
            waitLock();
            lock();
        }
    }

    private void waitLock() {
        IZkDataListener iZkDataListener = new IZkDataListener() {

            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {

            }

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                if (cdl != null) {
                    System.out.println("获取到通知");
                    cdl.countDown();
                }
            }
        };

        //注册前一个节点的监听事件
        zkClient.subscribeDataChanges(beforePath,iZkDataListener);

        if (zkClient.exists(beforePath)) {
            try {
                cdl.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //删除监听事件
        zkClient.unsubscribeDataChanges(beforePath,iZkDataListener);
    }

    private boolean tryLock() {
        if (currentPath == null) {
            currentPath = zkClient.createEphemeralSequential(parent + "/" + path, "lock");
        }
        List<String> children = zkClient.getChildren(parent);
        Collections.sort(children);
        System.out.println("==============================================:" + children);
        if (currentPath.equals(parent + "/" + children.get(0))) {
            return true;
        } else {
            int wz = Collections.binarySearch(children, currentPath.substring(parent.length() + 1));
            beforePath = parent + '/' + children.get(wz - 1);
            return false;
        }
    }



    public void unlock(){
        if (zkClient != null) {
            System.out.println(Thread.currentThread().getName() + ": 释放锁");
            zkClient.delete(currentPath);
            zkClient.close();
        }
    };



}





