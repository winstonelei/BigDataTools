package com.github.bigDataTools.zookeeper.curator;


import com.github.bigDataTools.zookeeper.Config;
import com.github.bigDataTools.zookeeper.ZkClient;
import com.github.bigDataTools.zookeeper.ZookeeperTransporter;

public class CuratorZookeeperTransporter implements ZookeeperTransporter {

    public ZkClient connect(Config config) {
        return new CuratorZkClient(config);
    }

}