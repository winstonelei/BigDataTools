package com.github.bigDataTools.zookeeper.zkClient;


import com.github.bigDataTools.zookeeper.Config;
import com.github.bigDataTools.zookeeper.ZkClient;
import com.github.bigDataTools.zookeeper.ZookeeperTransporter;

public class ZkClientZookeeperTransporter implements ZookeeperTransporter {

    public ZkClient connect(Config config) {
        return new ZkClientZkClient(config);
    }

}
