import com.github.bigDataTools.zookeeper.ZookeeperManager;
import org.apache.zookeeper.KeeperException;

/**
 * Created by dell on 2017/5/4.
 */
public class TestZk {

    public static  void main(String[] args){
        ZookeeperManager zkManager = ZookeeperManager.getInstance();
        try {
            zkManager.connect("114.55.253.15:2181",10000);
            //zkManager.createPersinstentNode("/demo/hello","word".getBytes());
            String str =new String(zkManager.getData("/demo/hello"));
            System.out.println(str);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
