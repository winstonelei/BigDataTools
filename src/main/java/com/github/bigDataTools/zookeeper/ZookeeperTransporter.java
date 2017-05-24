package com.github.bigDataTools.zookeeper;



public interface ZookeeperTransporter {

    ZkClient connect(Config config);

}
