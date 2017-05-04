import com.alibaba.fastjson.JSONObject;
import com.github.bigDataTools.es.EsSearchManager;
import com.github.bigDataTools.es.PageEntity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by dell on 2017/5/3.
 */
public class TestEs {
    public static  void main(String[] args){
        EsSearchManager esSearchManager = EsSearchManager.getInstance();
        try {
            List<Map<String,Object>> list = new ArrayList<>();
            Map<String,Object> map = new HashMap<>();
            map.put("name","shanghai");
            map.put("sex","woman");

            Map<String,Object> map1 = new HashMap<>();
            map1.put("name","beijing");
            map1.put("sex","woman");
            list.add(map);
            list.add(map1);

            // esSearchManager.buildList2Documents("testa","woman",list);//existsIndex("testaa");
            List<String> keywords = new ArrayList<>();
            keywords.add("beijing");

            List<String> types = new ArrayList<>();
            types.add("woman");
            //types.add("woman");
            List<String> indexs = new ArrayList<>();
            indexs.add("testa");

            List<String> fieldNames = new ArrayList<>();
            fieldNames.add("name");
    /*      PageEntity<JSONObject>  pg = esSearchManager.queryWithTerm(keywords,indexs,
                    types,fieldNames,null,null,null,null,1,10);*/
/*
            List<JSONObject> jsonList = pg.getContents();
            System.out.println(jsonList.toString());*/

     /*       List<Terms.Bucket> buckets = esSearchManager.queryAggByType(keywords,indexs,
                    types,fieldNames);

            System.out.println(buckets.size());*/

            PageEntity<JSONObject> pg  = esSearchManager.queryFulltext(keywords,
                    indexs, types, fieldNames,
                    null, 1,10);
            List<JSONObject> jsonList = pg.getContents();
            System.out.println(jsonList.toString());


        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
