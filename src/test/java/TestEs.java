import com.alibaba.fastjson.JSONObject;
import com.github.bigDataTools.es.EsSearchManager;
import com.github.bigDataTools.es.PageEntity;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by winstone on 2017/5/3.
 */
public class TestEs {

    @Test
    public void testEsQuery()throws  Exception{
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        EsSearchManager esSearchManager = EsSearchManager.getInstance();
        Client client = esSearchManager.client;
        long start = System.currentTimeMillis();
        SearchRequestBuilder searchReq = client.prepareSearch("fw_session_table_statistics_v1_*");
        searchReq.setTypes("fw_session_table_statistics_v1");
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        Date date=sdf.parse("2018-12-13 23:59:00");
        Date timeEnd = date;//model.getTimeBegin();
        Date  timeBegin= sdf.parse("2018-12-13 00:00:00");//addHour(timeEnd,-2);
        if (timeBegin != null || timeEnd != null) {
            RangeQueryBuilder timeRangeQuery = QueryBuilders.rangeQuery("collectTime");
            if (timeBegin != null) {
                timeRangeQuery.gte(timeBegin.getTime());
            }
            if (timeEnd != null) {
                timeRangeQuery.lte(timeEnd.getTime());
            }
            boolQueryBuilder.must(timeRangeQuery);//.must(QueryBuilders.termQuery("globalDeviceId",28));;
        }
        searchReq.setQuery(boolQueryBuilder).setSize(1000).addSort("collectTime", SortOrder.DESC);
        SearchResponse searchRes = searchReq.execute().actionGet();
        for (SearchHit searchHit : searchRes.getHits().getHits()) {
            System.out.println(sdf.format(new Date(Long.valueOf(searchHit.getSource().get("collectTime").toString())))+"="+searchHit.getSource());
        }
        System.out.println("总耗时：" + (System.currentTimeMillis()-start) + "ms"+ DateTimeZone.getDefault());

    }

    public static  void main(String[] args){

        EsSearchManager esSearchManager = EsSearchManager.getInstance();
        try {

            //esSearchManager.testAggreation();
           Map<String,String> fieldsInfo = new HashMap<>();
     /*       fieldsInfo.put("name","java.lang.String");
            fieldsInfo.put("age","java.lang.Integer");
            fieldsInfo.put("school","java.lang.String");
            List<String> excludeFields = new ArrayList<>();
            excludeFields.add("school");
            esSearchManager.buildIndexWithFields("exindex","exindex", fieldsInfo, excludeFields);
*/
           List<Map<String,Object>> list = new ArrayList<>();
            Map<String,Object> map = new HashMap<>();
            map.put("name","jack");
            map.put("age",50);
            map.put("school","ByteTCC Transaction Manager旨在提供一个兼容JTA的基于TCC机制的分布式事务管理器");

            Map<String,Object> map1 = new HashMap<>();
            map1.put("name","sony");
            map1.put("age",14);
            map1.put("school","支付宝在扣款事务提交之前，向实时消息服务请求发送消息，实时消息服务只记录消息数据，而不真正发送，只有消息发送成功后才会提交事务");
            list.add(map1);
            list.add(map);
            esSearchManager.buildList2Documents("testindex","testindex",list);

            List<String> keywords = new ArrayList<>();
            keywords.add("ByteTCC");
            List<String> types = new ArrayList<>();
            types.add("testindex");
            List<String> indexs = new ArrayList<>();
            indexs.add("testindex");
            List<String> fieldNames = new ArrayList<>();
            fieldNames.add("school");

            PageEntity<JSONObject> pg = esSearchManager.queryFulltext(keywords,
                    indexs, types, fieldNames, null, 1,10);

     /*       PageEntity<JSONObject> pg = esSearchManager.queryWithTerm(keywords, indexs,
                    types, fieldNames, null, null, null, null, 1, 10);
                    */

            List<JSONObject> jsonList = pg.getContents();

            System.out.println(jsonList);

/*
            List<Map<String,Object>> list = new ArrayList<>();
            Map<String,Object> map = new HashMap<>();
            map.put("name","mike");
            map.put("home","usa");
            map.put("now_home","china");
            map.put("height",33);
            map.put("age",20);
            map.put("birthday",new SimpleDateFormat("YYYYMMddHHmmss").format(new Date()));
            map.put("isRealMen",true);

            Map<String,Object> map1 = new HashMap<>();
            map1.put("name","xap");
            map1.put("home","france");
            map1.put("now_home","france");
            map1.put("height",23);
            map1.put("age",55);
            map1.put("birthday",new SimpleDateFormat("YYYYMMddHHmmss").format(new Date()));
            map1.put("isRealMen",true);
            list.add(map1);
            list.add(map);
             esSearchManager.buildList2Documents("tempindex","tempindex",list);
*/

            /*     List<Map<String,Object>> list = new ArrayList<>();
            Map<String,Object> map = new HashMap<>();
            map.put("fieldA",80);
            map.put("fieldB",30);
            map.put("fieldC","hoge");
            map.put("fieldD","huga");
            list.add(map);
            JSONObject jsonObject = new JSONObject(map);*/
          //  esSearchManager.buildIndex("testindex","testtypes");
            //esSearchManager.buildList2Documents("testindex","testtypes",list);
            //esSearchManager.buildDocument("testindex","testtypes","666",jsonObject.toString());
          //  esSearchManager.buildList2Documents("testindex","testtypes",list);
         /*   PageEntity<JSONObject> pg  = esSearchManager.queryFulltext(keywords,
                    indexs, types, fieldNames,
                    null, 1,10);
            List<JSONObject> jsonList = pg.getContents();
            System.out.println(jsonList.toString());*/

            //esSearchManager.rangeQuery("testindex","testtypes");
      /*


*/
        } catch (Exception e) {
            e.printStackTrace();
        }


    /*public void  test(){
        SearchRequestBuilder searchRequestBuilder = getClient().prepareSearch("testindex").setTypes("testtype");
        SearchResponse searchResponse=searchRequestBuilder.setQuery(QueryBuilders.matchPhraseQuery("fieldD", "bigData is magic"))
                .setFrom(0).setSize(10).setExplain(true).execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        for (int i = 0; i < hits.getHits().length; i++) {
            System.out.println(hits.getHits()[i].getSourceAsString());
        }

        //matchPhraseQuery(field,text)函数，这个函数的参数有两个,其中对应text的部分是要解析的，例如，bigData is magic可能经过解析之后会解析成molong1208 以及blog然后再进行查询的
        searchRequestBuilder.setQuery(QueryBuilders.matchPhraseQuery("fieldD", "bigData is magic"));

        //词精确查询，fieldD 分词后包含 bigData的term的文档
        searchRequestBuilder.setQuery(QueryBuilders.termQuery("fieldD", "bigData"));

        //terms Query 多term查询，查询fieldD 包含 bigData  spark或storm 中的任何一个或多个的文档
        searchRequestBuilder.setQuery(QueryBuilders.termsQuery("fieldD", "bigData","spark","storm"));

        //范围查询字段fieldB 大于20并且小于50 包含上下界
        searchRequestBuilder.setQuery(QueryBuilders.rangeQuery("filedB")
                .gt("20").lt("50").includeLower(true).includeUpper(true));

        // prefix query 匹配分词前缀 如果字段没分词，就匹配整个字段前缀
        searchRequestBuilder.setQuery(QueryBuilders.prefixQuery("fieldD","spark"));

        //wildcard query 通配符查询，支持* 任意字符串；？任意一个字符
        searchRequestBuilder.setQuery(QueryBuilders.wildcardQuery("fieldD","spark*"));

        //Fuzzy query 分词模糊查询，通过增加fuzziness 模糊属性，来查询term 如下 能够匹配 fieldD 为 spar park spark前或后加一个字母的term的 文档 fuzziness 的含义是检索的term 前后增加或减少n个单词的匹配查询，
        searchRequestBuilder.setQuery(QueryBuilders.fuzzyQuery("fieldD","spark").fuzziness(Fuzziness.ONE));

    }*/

    }
}
