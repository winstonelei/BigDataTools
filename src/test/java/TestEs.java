import com.alibaba.fastjson.JSONObject;
import com.github.bigDataTools.es.EsSearchManager;
import com.github.bigDataTools.es.PageEntity;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by winstone on 2017/5/3.
 */
public class TestEs {

    public static  void main(String[] args){

        EsSearchManager esSearchManager = EsSearchManager.getInstance();
        try {
            List<Map<String,Object>> list = new ArrayList<>();
            Map<String,Object> map = new HashMap<>();
            map.put("fieldA",80);
            map.put("fieldB",30);
            map.put("fieldC","hoge");
            map.put("fieldD","huga");
            list.add(map);
            JSONObject jsonObject = new JSONObject(map);
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

            List<String> keywords = new ArrayList<>();
            keywords.add("huga");

            List<String> types = new ArrayList<>();
            types.add("testtypes");

            List<String> indexs = new ArrayList<>();
            indexs.add("testindex");

            List<String> fieldNames = new ArrayList<>();
            fieldNames.add("fieldD");

            PageEntity<JSONObject>  pg = esSearchManager.queryWithTerm(keywords,indexs,
                    types,fieldNames,null,null,null,null,1,10);

            List<JSONObject> jsonList = pg.getContents();
            System.out.println(jsonList.toString());


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
