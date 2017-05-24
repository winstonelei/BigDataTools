package com.github.bigDataTools.util.lock;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ResourceBundle;
/**
 * 锁服务
 * @author  winstone
 */
public class LockService {
	private static final Logger LOG = LoggerFactory.getLogger(LockService.class);
	 //确保所有线程运行结束；
	private static  String CONNECTION_STRING ;
	private static String GROUP_PATH ;
	private static String SUB_PATH ;
	private static int SESSION_TIMEOUT ;

	public void doService(ServiceTemplate doTemplate) {
		try {
			AbstractZooKeeper az = new AbstractZooKeeper();
			ResourceBundle rb = ResourceBundle.getBundle("commons");
			CONNECTION_STRING = rb.getString("connectString");
			SESSION_TIMEOUT = Integer.parseInt(rb.getString("sessionTimeOut"));
			GROUP_PATH = rb.getString("groupPath");
			SUB_PATH = rb.getString("subPath");
			ZooKeeper zk = az.connect(CONNECTION_STRING, SESSION_TIMEOUT);
			DistributedLock dc = new DistributedLock(zk,GROUP_PATH,SUB_PATH);
			LockWatcher lockWatcher = new LockWatcher(dc, doTemplate);
			dc.setWatcher(lockWatcher);
			dc.createPath(GROUP_PATH, "该节点由线程" + Thread.currentThread().getName() + "创建");
			boolean rs = dc.getLock();
			if (rs == true) {
				lockWatcher.doWork();
				dc.unlock();
			}
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
	}
}
