import com.github.bigDataTools.hbase.HbaseManager;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by winstone on 2017/5/4.
 */
public class TestHbase2 {

    @Test
    public void testQuery() throws  Exception{
        HbaseManager util = HbaseManager.getInstance();
       // Map<String, String> map =  util.getRowValue("gz2lz","666","ceshi");//0e50de0ca627030d
        Map<String, String> map =  util.getRowValue("wafloggz","0e50de0ca627030d","ceshi");//0e50de0ca627030d
        Iterator<Map.Entry<String, String>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, String> entry = iterator.next();
            System.out.println(entry.getKey()+"="+entry.getValue());
        }
    }

    @Test
    public void testMultiInsert() throws  Exception{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dateTime  = sdf.format(new Date());
        System.out.println(dateTime);
        HbaseManager util = HbaseManager.getInstance();
        String tableName ="wafloggz";
        try {
            Table t = HbaseManager.getConn().getTable(TableName.valueOf(tableName));
            long startTime = System.currentTimeMillis();
            System.out.println("start time ="+startTime);
            for(int i=0;i<10000;i++){
                String rowKey= MD5Hash.getMD5AsHex(Bytes.toBytes(System.currentTimeMillis()));
                int rowKeyLength = rowKey.length()/2;
                Map<String,Object> events = new HashMap<>();
                events.put("schema","http");
                events.put("method","GET");
                events.put("timeType","1");
                events.put("sessionId","sessionId");
                events.put("serverPort","2041");
                events.put("trendyType","12");
                events.put("geolocationCountryCode","CN");
                events.put("url","/static/js/tj.js");
                events.put("requestDatetime",sdf.format(new Date()));
                events.put("policyId","-jRYUcDXrdO0UKM8YDBYMw");
                events.put("clientIp","121.207.111.122");
                events.put("domain","122.gov.cn");
                events.put("domainName","fj");
                events.put("requestPolicyReference","{\"link\":\"https://localhost/mgmt/tm/asm/policies/-jRYUcDXrdO0UKM8YDBYMw?ver=12.1.2\"}");
                events.put("clientPort","28112");
                events.put("customerId","1331");
                long currentTime = new Date().getTime();
                events.put("fromTime",currentTime);
                events.put("serverIp","10.0.217.76");
                events.put("enforcementState","28112");
                events.put("time",currentTime);
                events.put("toTime",currentTime);
                util.insertMultiNew(t,rowKey.substring(0,rowKeyLength),"ceshi",events);
            }
            long endTime = System.currentTimeMillis();
            System.out.println("end time = "+endTime);
            System.out.println("cost time = "+(endTime-startTime));
            HbaseManager.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

/*
    @Test
    public void testSplitTable() throws  Exception{
        HbaseManager util = HbaseManager.getInstance();
        util.createTableWithSplits("wafsplit","ceshi");
        util.createTableWithSplits("wafgzsplit","ceshi");
    }
*/


    @Test
    public void testWafInsert() throws  Exception{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dateTime  = sdf.format(new Date());
        System.out.println(dateTime);
        HbaseManager util = HbaseManager.getInstance();
        int count = 0;
        try {
            long startTime = System.currentTimeMillis();
            System.out.println("start time ="+startTime);
            List<Put> list = new ArrayList<>();
            List<Put> gzList = new ArrayList<>();
            for(int i=0;i<10000;i++){
                String rowKey= MD5Hash.getMD5AsHex(Bytes.toBytes(System.currentTimeMillis()));
                int rowKeyLength = rowKey.length()/2;
                Map<String,Object> events = new HashMap<>();
                events.put("schema","http");
                events.put("method","GET");
                events.put("timeType","1");
                events.put("sessionId","sessionId");
                events.put("serverPort","2041");
                events.put("trendyType","12");
                events.put("geolocationCountryCode","CN");
                events.put("url","/static/js/tj.js");
                events.put("requestDatetime",sdf.format(new Date()));
                events.put("policyId","-jRYUcDXrdO0UKM8YDBYMw");
                events.put("clientIp","121.207.111.122");
                events.put("domain","122.gov.cn");
                events.put("domainName","fj");
                events.put("requestPolicyReference","{\"link\":\"https://localhost/mgmt/tm/asm/policies/-jRYUcDXrdO0UKM8YDBYMw?ver=12.1.2\"}");
                events.put("clientPort","28112");
                events.put("customerId","1331");
                long currentTime = new Date().getTime();
                events.put("fromTime",currentTime);
                events.put("serverIp","10.0.217.76");
                events.put("enforcementState","28112");
                events.put("time",currentTime);
                events.put("toTime",currentTime);
                //list = util.insertMultis(list,"waflog",rowKey.substring(0,rowKeyLength),"ceshi",events);
                gzList = util.insertMultis(gzList,rowKey.substring(0,rowKeyLength),"ceshi",events);
                if(gzList.size()%100==0){
                    count++;
                    System.out.println("当前行"+i+"进入判断list大小"+gzList.size()+",计数值="+count);
                    util.insertList(gzList,"wafloggz");
                    gzList.clear();
                }
            }
            long endTime = System.currentTimeMillis();
            System.out.println("end time = "+endTime);
            System.out.println("cost time = "+(endTime-startTime));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSyncPatchWafInsert() throws  Exception{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dateTime  = sdf.format(new Date());
        System.out.println(dateTime);
        HbaseManager util = HbaseManager.getInstance();
        int count = 0;
        try {
            long startTime = System.currentTimeMillis();
            System.out.println("start time ="+startTime);
            List<Put> list = new ArrayList<>();
            List<Put> gzList = new ArrayList<>();
            for(int i=0;i<200;i++){
                String rowKey= MD5Hash.getMD5AsHex(Bytes.toBytes(System.currentTimeMillis()))+i;
                //int rowKeyLength = rowKey.length()/2;
                Map<String,Object> events = new HashMap<>();
                events.put("schema","http");
                events.put("method","GET");
                events.put("timeType","1");
                events.put("sessionId","sessionId");
                events.put("serverPort","2041");
                events.put("trendyType","12");
                events.put("geolocationCountryCode","CN");
                events.put("url","/static/js/tj.js");
                events.put("requestDatetime",sdf.format(new Date()));
                events.put("policyId","-jRYUcDXrdO0UKM8YDBYMw");
                events.put("clientIp","121.207.111.122");
                events.put("domain","122.gov.cn");
                events.put("domainName","fj");
                events.put("requestPolicyReference","{\"link\":\"https://localhost/mgmt/tm/asm/policies/-jRYUcDXrdO0UKM8YDBYMw?ver=12.1.2\"}");
                events.put("clientPort","28112");
                events.put("customerId","1331");
                long currentTime = new Date().getTime();
                events.put("fromTime",currentTime);
                events.put("serverIp","10.0.217.76");
                events.put("enforcementState","28112");
                events.put("time",currentTime);
                events.put("toTime",currentTime);
                gzList = util.insertMultis(gzList,rowKey,"ceshi",events);
            }
            util.batchPut("testBatch",gzList,true);
            long endTime = System.currentTimeMillis();
            System.out.println("end time = "+endTime);
            System.out.println("cost time = "+(endTime-startTime));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testAsyncPatchWafInsert() throws  Exception{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dateTime  = sdf.format(new Date());
        HbaseManager util = HbaseManager.getInstance();
        int count = 0;
        try {
            long startTime = System.currentTimeMillis();
            System.out.println("start time ="+startTime);
            List<Put> list = new ArrayList<>();
            List<Put> gzList = new ArrayList<>();
            for(int i=0;i<200;i++){
                String rowKey= MD5Hash.getMD5AsHex(Bytes.toBytes(System.currentTimeMillis()))+i;
                //int rowKeyLength = rowKey.length()/2;
                Map<String,Object> events = new HashMap<>();
                events.put("schema","http");
                events.put("method","GET");
                events.put("timeType","1");
                events.put("sessionId","sessionId");
                events.put("serverPort","2041");
                events.put("trendyType","12");
                events.put("geolocationCountryCode","CN");
                events.put("url","/static/js/tj.js");
                events.put("requestDatetime",sdf.format(new Date()));
                events.put("policyId","-jRYUcDXrdO0UKM8YDBYMw");
                events.put("clientIp","121.207.111.122");
                events.put("domain","122.gov.cn");
                events.put("domainName","fj");
                events.put("requestPolicyReference","{\"link\":\"https://localhost/mgmt/tm/asm/policies/-jRYUcDXrdO0UKM8YDBYMw?ver=12.1.2\"}");
                events.put("clientPort","28112");
                events.put("customerId","1331");
                long currentTime = new Date().getTime();
                events.put("fromTime",currentTime);
                events.put("serverIp","10.0.217.76");
                events.put("enforcementState","28112");
                events.put("time",currentTime);
                events.put("toTime",currentTime);
                gzList = util.insertMultis(gzList,rowKey,"ceshi",events);
            }
            util.batchAsyncPut("testBatch",gzList,true);
            long endTime = System.currentTimeMillis();
            System.out.println("end time = "+endTime);
            System.out.println("cost time = "+(endTime-startTime));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
