import com.github.bigDataTools.hbase.HbaseManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.junit.Test;
import scala.collection.parallel.mutable.ParArray;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by dell on 2017/5/4.
 */
public class TestHbase {


    @Test
    public void testQuery() throws  Exception{
        HbaseManager util = HbaseManager.getInstance();
        Map<String, String> map =  util.getRowValue("firsttable","0611b1fc93fc5a87","ceshi");
        Iterator<Map.Entry<String, String>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, String> entry = iterator.next();
            System.out.println(entry.getKey()+"="+entry.getValue());
        }
    }

    @Test
    public void testInsert() throws  Exception{
        SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMdd");
        String dateTime  = sdf.format(new Date());
        System.out.println(dateTime);
        HbaseManager util = HbaseManager.getInstance();
        try {
            //  util.dropTable("testsplit");
            // util.createTable("firsttable","ceshi");
            long startTime = System.currentTimeMillis();
            System.out.println("start time ="+startTime);
            for(int i=10;i<100000;i++){
                String rowKey= MD5Hash.getMD5AsHex(Bytes.toBytes(System.currentTimeMillis()));
                int rowKeyLength = rowKey.length()/2;
                System.out.println(rowKey);
                util.insert("firsttable",rowKey.substring(0,rowKeyLength),"ceshi","name","xiaoming"+i);
                util.insert("firsttable",rowKey.substring(0,rowKeyLength),"ceshi","sex","xiaoming"+i);
            }
            long endTime = System.currentTimeMillis();
            System.out.println("end time = "+endTime);
            System.out.println("cost time = "+(endTime-startTime));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
