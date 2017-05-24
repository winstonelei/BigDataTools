import com.github.bigDataTools.util.lock.ZookeeperManager;
import com.github.bigDataTools.zookeeper.Config;
import com.github.bigDataTools.zookeeper.ZkClient;
import com.github.bigDataTools.zookeeper.ZookeeperTransporter;
import com.github.bigDataTools.zookeeper.curator.CuratorZookeeperTransporter;
import com.github.bigDataTools.zookeeper.zkClient.ZkClientZookeeperTransporter;

import java.util.List;

/**
 * Created by winstone on 2017/5/4.
 */
public class TestZk {

    public static  void main(String[] args){
        /*ZookeeperManager zkManager = ZookeeperManager.getInstance();
        try {
            zkManager.connect("114.55.253.15:2181",10000);
            //zkManager.createPersinstentNode("/demo/hello","word".getBytes());
            String str =new String(zkManager.getData("/demo/hello"));
            System.out.println(str);
        } catch (Exception e) {
            e.printStackTrace();
        }*/

        ZookeeperTransporter  zk = new CuratorZookeeperTransporter();
        Config config = new Config();
        config.setRegistryAddress("114.55.253.15:2181");
        ZkClient zkClient  = zk.connect(config);

        List<String> list = zkClient.getChildren("/LTS");

        for(String str : list){
            System.out.println(str);
        }




    }
}
