package com.github.bigDataTools.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 锁观察者，当锁下面的目录发生变化时触发process相关操作
 * @author  winstone
 */
public class LockWatcher implements Watcher {

	private static final Logger LOG = LoggerFactory.getLogger(LockWatcher.class);
	private String waitPath;
	private DistributedLock distributedLock;
	private ServiceTemplate doTemplate;

	public LockWatcher(DistributedLock distributedLock,ServiceTemplate doTemplate) {
		this.distributedLock = distributedLock;
		this.doTemplate = doTemplate;
	}
	
	
	@Override
	public void process(WatchedEvent event) {
		// 判断当前event是否是删除事件，并且得到节点状态是
		if (event.getType() == Event.EventType.NodeDeleted && event.getPath().equals(distributedLock.getWaitPath())) {
            LOG.info(Thread.currentThread().getName()+ "前面线程已经释放锁，检查是否获取锁");
            try {  
                if(distributedLock.checkMinPath()){  
                	doWork();
                	distributedLock.unlock();
                }  
            } catch ( Exception e) {  
                e.printStackTrace();  
            }  
        } 
	}
	
	public   void doWork(){
	    LOG.info(Thread.currentThread().getName() + "获取锁成功,开始业务逻辑处理！");
	    doTemplate.doService();

	}

}
