import com.alibaba.fastjson.JSONObject;
import com.github.bigDataTools.es.EsSearchManager;
import com.github.bigDataTools.es.PageEntity;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by winstone on 2017/6/3 0003.
 */
public class TestIndexWithMapping {

        public static Date getNextDay(Date date) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            calendar.add(Calendar.DAY_OF_MONTH, -1);
            date = calendar.getTime();
            return date;
        }

        public static Date getToday(Date date) {
            Calendar cal = Calendar.getInstance();
            cal.set(Calendar.HOUR_OF_DAY,0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.add(Calendar.DATE,0);
            return date;
    }

    public static  void main(String[] args) {

        EsSearchManager esSearchManager = EsSearchManager.getInstance();

        try {

   /*     XContentBuilder mapping = XContentFactory.jsonBuilder()
                    .startObject()
                    //properties下定义的name等等就是属于我们需要的自定义字段了,相当于数据库中的表字段 ,此处相当于创建数据库表
                    .startObject("properties")
                    .startObject("name").field("type", "string").field("store", "yes").endObject()
                    .startObject("home").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("now_home").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("height").field("type", "double").endObject()
                    .startObject("age").field("type", "integer").endObject()
                    .startObject("birthday").field("type", "date").field("format", "YYYYMMddHHmmss").endObject()
                    .startObject("isRealMen").field("type", "boolean").endObject()
                    .endObject()
                    .endObject();

            esSearchManager.buildIndexWithMapping("tempindex","tempindex",mapping);*/

            List<String> keywords = new ArrayList<>();
            keywords.add("france");
            List<String> types = new ArrayList<>();
            types.add("tempindex");
            List<String> indexs = new ArrayList<>();
            indexs.add("tempindex");
            List<String> fieldNames = new ArrayList<>();
            fieldNames.add("home");

            SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMddHHmmss");
            Long beinTime = Long.valueOf(sdf.format(getNextDay(new Date())));
            Long endTime = Long.valueOf(sdf.format(getToday(new Date())));

            List<String> dateFields = new ArrayList<>();
            dateFields.add("birthday");

            List<String> allFields = new ArrayList<>();
            allFields.add("name");
            allFields.add("home");
            allFields.add("name");
            allFields.add("height");
            allFields.add("age");
            allFields.add("now_home");
            allFields.add("isRealMen");
            allFields.add("birthday");

            PageEntity<JSONObject> pg = esSearchManager.queryWithTerm(keywords, indexs,
                    types, fieldNames, dateFields, allFields, beinTime, endTime, 1, 10);

            List<JSONObject> jsonList = pg.getContents();
            System.out.println(jsonList.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
