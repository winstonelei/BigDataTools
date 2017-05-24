package com.github.bigDataTools.util.lock;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * zk 链接器
 */
public class AbstractZooKeeper implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractZooKeeper.class);

    protected ZooKeeper zooKeeper;
    protected CountDownLatch countDownLatch = new CountDownLatch(1);  

    public ZooKeeper connect(String hosts, int SESSION_TIMEOUT) throws IOException, InterruptedException{
        zooKeeper = new ZooKeeper(hosts,SESSION_TIMEOUT,this);
        countDownLatch.await();
        LOG.info("AbstractZooKeeper.connect()");
        return zooKeeper;
    }  
    
    public void process(WatchedEvent event) {
        if(event.getState() == KeeperState.SyncConnected){
            countDownLatch.countDown();  
        }  
    }  
    public void close() throws InterruptedException{  
        zooKeeper.close();  
    }  
}  