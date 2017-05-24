import com.alibaba.fastjson.JSONObject;
import com.github.bigDataTools.es.EsSearchManager;
import com.github.bigDataTools.es.PageEntity;

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
            map.put("fieldA",100);
            map.put("fieldB",22);
            map.put("fieldC","hoge");
            map.put("fieldD","huga");
            list.add(map);
          //  JSONObject jsonObject = new JSONObject(map);
           // esSearchManager.buildIndex("testindex","testtypes");
           // esSearchManager.buildDocument("testindex","testtypes","1111",jsonObject.toJSONString());//existsIndex("testaa");
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

    }
}
