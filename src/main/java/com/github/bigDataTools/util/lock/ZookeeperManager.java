package com.github.bigDataTools.util.lock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by winstone on 2017/5/4.
 */
public class ZookeeperManager extends  AbstractZooKeeper {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperManager.class);

    private static ZookeeperManager zkManager;

    private ZookeeperManager(){}

    public static ZookeeperManager getInstance(){
        if( null == zkManager){
            synchronized (ZookeeperManager.class){
                zkManager = new ZookeeperManager();
            }
        }
        return zkManager;
    }

    /**
     * 创建永久znode
     * @param path
     * @param data
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void createPersinstentNode(String path,byte[] data) throws KeeperException, InterruptedException{
        /**
         * 此处采用的是创建的是持久化节点：PERSISTENT表示不会因连接的断裂而删除节点
         * EPHEMERAL 表示The znode will be deleted upon the client's disconnect.
         */
        this.zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * 创建临时节点
     * @param path
     * @param data
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void createEphemeralNode(String path,byte[] data) throws KeeperException, InterruptedException{
        /**
         * 此处采用的是创建的是持久化节点：PERSISTENT表示不会因连接的断裂而删除节点
         * EPHEMERAL 表示The znode will be deleted upon the client's disconnect.
         */
        this.zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }


    /**
     * 获取节点数据
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public byte[] getData(String path) throws KeeperException, InterruptedException {
        return  this.zooKeeper.getData(path, false,null);
    }


    /**
     * 修改节点数据
     * @param path
     * @param data
     * @param version
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException{
        return this.zooKeeper.setData(path, data, version);
    }

    /**
     * 删除节点数据
     * @param path
     * @param version
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void deleteNode(final String path,int version) throws InterruptedException, KeeperException{
        this.zooKeeper.delete(path, version);
    }


    /**
     * 获取节点数据
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public List<String> getChild(String path) throws KeeperException, InterruptedException{
        List<String> childList = new ArrayList<String>();
        try{
            List<String> list=this.zooKeeper.getChildren(path, false);
            if(list.isEmpty()){
                 LOG.info(path+"中没有节点");
            }else{
                for(String child:list){
                 LOG.info(child);
                 childList.add(child);
                }
            }
        }catch (KeeperException.NoNodeException e) {
            // TODO: handle exception
              LOG.error(e.getMessage());
              throw e;
        }
        return childList;
    }
}
