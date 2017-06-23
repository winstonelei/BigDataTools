import com.github.bigDataTools.es.EsSearchManager;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.max.MaxBuilder;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.min.MinBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wistone  on 2017/5/24.
 */
public class TestEsAggreation {


    public static void main(String[] args){
        EsSearchManager esSearchManager = EsSearchManager.getInstance();
        try {
            SearchRequestBuilder searchReq = esSearchManager.client.prepareSearch("tempindex");
            searchReq.setTypes("tempindex");

            TermsBuilder termsb = AggregationBuilders.terms("my_fieldA").field("fieldA").size(100);

            //过滤条件
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            boolQueryBuilder.must(QueryBuilders.rangeQuery("fieldA").from(20).to(100));

            searchReq.setQuery(boolQueryBuilder).addAggregation(termsb);
            SearchResponse searchRes = searchReq.execute().actionGet();

            Terms fieldATerms = searchRes.getAggregations().get("my_fieldA");
            for (Terms.Bucket filedABucket : fieldATerms.getBuckets()) {
                //fieldA
                String fieldAValue = filedABucket.getKey().toString();

                //COUNT(fieldA)
                long fieldACount = filedABucket.getDocCount();

                System.out.println("fieldAValue="+fieldAValue);

                System.out.println("fieldACount="+fieldACount);
            }

            /*for (Terms.Bucket filedABucket : fieldATerms.getBuckets()) {
                //fieldA
                String key = filedABucket.getKey().toString();
                //COUNT(fieldA)
                long  value = filedABucket.getDocCount();

                System.out.println("fieldAValue="+key);

                System.out.println("fieldACount="+value);
            }
*/


            /*XContentBuilder mapping = XContentFactory.jsonBuilder()
                    .startObject()
                    //properties下定义的name等等就是属于我们需要的自定义字段了,相当于数据库中的表字段 ,此处相当于创建数据库表
                    .startObject("properties")
                    .startObject("fieldA").field("type", "integer").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("fieldB").field("type", "integer").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("fieldC").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("fieldD").field("type", "string").endObject()
                    .endObject();

            esSearchManager.buildIndexWithMapping("tempindex", "tempindex", mapping);*/


 /*
            List<Map<String,Object>> list = new ArrayList<>();
            Map<String,Object> map1 = new HashMap<>();
            map1.put("fieldA",10);
            map1.put("fieldB",20);
            map1.put("fieldC","java");
            map1.put("fieldD","java 编程技术");

            Map<String,Object> map2 = new HashMap<>();
            map2.put("fieldA",15);
            map2.put("fieldB",25);
            map2.put("fieldC","ptyhone");
            map2.put("fieldD","python算法");


            Map<String,Object> map3 = new HashMap<>();
            map3.put("fieldA",25);
            map3.put("fieldB",35);
            map3.put("fieldC","c++");
            map3.put("fieldD","c++核心思想");
*/

       /*     Map<String,Object> map4 = new HashMap<>();
            map4.put("fieldA",10);
            map4.put("fieldB",20);
            map4.put("fieldC","java");
            map4.put("fieldD","java基础语法");

            Map<String,Object> map5 = new HashMap<>();
            map5.put("fieldA",10);
            map5.put("fieldB",20);
            map5.put("fieldC","java");
            map5.put("fieldD","spark大数据");

            *//*list.add(map1);
            list.add(map2);
            list.add(map3);
*//*
            list.add(map4);
            list.add(map5);

            esSearchManager.buildList2Documents("tempindex","tempindex",list);
*/
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /*
     XContentBuilder mapping = XContentFactory.jsonBuilder()
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

   /*
     范围查询，es中 索引类型一定要指定好，例如根据fieldB的范围查询，则fieldB一定是数值类型
   public  void rangeQuery( String index, String type) {
        // Query
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("fieldB");
        rangeQueryBuilder.gte(0);
        rangeQueryBuilder.lte(100);
        // Search
        SearchRequestBuilder searchRequestBuilder = EsSearchManager.getInstance().getClient().prepareSearch(index);
        searchRequestBuilder.setTypes(type);
        searchRequestBuilder.setQuery(rangeQueryBuilder);
        // 执行
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        // 结果
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSource().toString());
        }

    }
    */

    public void  queryByArrage(){




	  /*
        设置索引字段mapping
        XContentBuilder mapping = XContentFactory.jsonBuilder()
        .startObject()
        //properties下定义的name等等就是属于我们需要的自定义字段了,相当于数据库中的表字段 ,此处相当于创建数据库表
        .startObject("properties")
        .startObject("fieldA").field("type", "integer").field("store", "yes").field("index", "not_analyzed").endObject()
        .startObject("fieldB").field("type", "integer").field("store", "yes").field("index", "not_analyzed").endObject()
        .startObject("fieldC").field("type", "string").field("index", "not_analyzed").endObject()
        .startObject("fieldD").field("type", "string").endObject()
        .endObject();*/

	 /*
	    按某个field group by查询count
		SELECT
		fieldA, COUNT(fieldA)
		from table
		WHERE fieldC = "hoge"
		AND fieldD = "huga"
		AND fieldB > 10
		AND fieldB < 100
		group by fieldA;


	  SearchRequestBuilder searchReq = client.prepareSearch("testindex");
	  searchReq.setTypes("testtypes");
	  //group by 条件
	  TermsBuilder termsb = AggregationBuilders.terms("my_fieldA").field("fieldA").size(100);

     //过滤条件
	  BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
	  boolQueryBuilder.must(QueryBuilders.termQuery("fieldC", "hoge"));
	  boolQueryBuilder.must(QueryBuilders.rangeQuery("fieldB").from(20).to(100));
	  boolQueryBuilder.must(QueryBuilders.termQuery("fieldD", "huga"));

	  searchReq.setQuery(boolQueryBuilder).addAggregation(
			  termsb);
	  SearchResponse searchRes = searchReq.execute().actionGet();

	  Terms fieldATerms = searchRes.getAggregations().get("my_fieldA");
	  for (Terms.Bucket filedABucket : fieldATerms.getBuckets()) {
		  //fieldA
		  String fieldAValue = filedABucket.getKey().toString();

		  //COUNT(fieldA)
		  long fieldACount = filedABucket.getDocCount();

		  System.out.println("fieldAValue="+fieldAValue);

		  System.out.println("fieldACount="+fieldACount);
	  }

*/



/*
	  按两个field group by并查询第三个filed的sum
	  SELECT
	  fieldA, fieldC, SUM(fieldB)
	  from table
	  group by fieldA, fieldC;

	  SearchRequestBuilder searchReq = client.prepareSearch("testindex");
	  searchReq.setTypes("testtypes");

	  TermsBuilder termsb_fa = AggregationBuilders.terms("my_fieldA").field("fieldA").size(100);
	  TermsBuilder termsb_fc = AggregationBuilders.terms("my_fieldC").field("fieldC").size(50);

	  termsb_fc.subAggregation(AggregationBuilders.sum("my_sum_fieldB").field("fieldB"));
	  termsb_fa.subAggregation(termsb_fc);

	  searchReq.setQuery(QueryBuilders.matchAllQuery()).addAggregation(termsb_fa);
	  SearchResponse searchRes = searchReq.execute().actionGet();

	  Terms fieldATerms = searchRes.getAggregations().get("my_fieldA");
	  for (Terms.Bucket filedABucket : fieldATerms.getBuckets()) {
		  //fieldA
		  String fieldAValue = filedABucket.getKey().toString();
		  System.out.println("fieldAValue="+fieldAValue);
		  Terms fieldCTerms = filedABucket.getAggregations().get("my_fieldC");
		  for (Terms.Bucket filedCBucket : fieldCTerms.getBuckets()) {
			  //fieldC
			  String fieldCValue = filedCBucket.getKey().toString();
			  System.out.println("fieldCValue="+fieldCValue);
			  //SUM(fieldB)
			  Sum sumagg = filedCBucket.getAggregations().get("my_sum_fieldB");
			  long sumFieldB = (long)sumagg.getValue();
			  System.out.println("sumFieldB="+sumFieldB);
		  }
	  }*/



	/*
	* 按某个filed group by 并查询count、sum 和 average
	*
	*  SELECT
	  fieldA, COUNT(fieldA), SUM(fieldB), AVG(fieldB)
	  from table
	  group by fieldA;
	  *//*
	  SearchRequestBuilder searchReq = client.prepareSearch("testindex");
	  searchReq.setTypes("testtypes");

	  TermsBuilder termsb = AggregationBuilders.terms("my_fieldA").field("fieldA").size(100);
	  termsb.subAggregation(AggregationBuilders.sum("my_sum_fieldB").field("fieldB"));
	  termsb.subAggregation(AggregationBuilders.avg("my_avg_fieldB").field("fieldB"));

	  searchReq.setQuery(QueryBuilders.matchAllQuery()).addAggregation(termsb);
	  SearchResponse searchRes = searchReq.execute().actionGet();
	  Terms fieldATerms = searchRes.getAggregations().get("my_fieldA");
	  for (Terms.Bucket filedABucket : fieldATerms.getBuckets()) {
		  //fieldA
		  String fieldAValue = filedABucket.getKey().toString();

		  //COUNT(fieldA)
		  long fieldACount = filedABucket.getDocCount();

		  //SUM(fieldB)
		  Sum sumagg = filedABucket.getAggregations().get("my_sum_fieldB");
		  long sumFieldB = (long)sumagg.getValue();

		  //AVG(fieldB)
		  Avg avgagg = filedABucket.getAggregations().get("my_avg_fieldB");
		  double avgFieldB = avgagg.getValue();

		  System.out.println("fieldAValue="+fieldAValue);
		  System.out.println("fieldACount="+fieldACount);
		  System.out.println("sumFieldB="+sumFieldB);
		  System.out.println("avgFieldB="+avgFieldB);
	  }*/


	/* *//* 按某个field group by 并按另一个filed的Sum排序，获取前10
	  SELECT
	  fieldA, SUM(fieldB)
	  from table
	  WHERE fieldC = "hoge"
	  group by fieldA
	  order by SUM(fieldB) DESC
	  limit 10;*//*
      //设置查询条件
	  QueryBuilder termsc = QueryBuilders.termQuery("fieldC","hoge");
	  QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(termsc);
      //设置groupby字段
	  TermsBuilder aggregationBuilder = AggregationBuilders.terms("my_fieldA").field("fieldA").size(10);
	  aggregationBuilder.subAggregation(AggregationBuilders.sum("my_sum_fieldB").field("fieldB"));
	  //设置orderby 字段
	  aggregationBuilder.order(Terms.Order.aggregation("my_sum_fieldB", false));
	  SearchResponse searchResponse = client.prepareSearch("testindex").setQuery(queryBuilder).addAggregation(aggregationBuilder).execute().actionGet();
	  Terms terms = searchResponse.getAggregations().get("my_fieldA");
	  for (Terms.Bucket entry : terms.getBuckets()) {
		  String fieldAValue = entry.getKey().toString();

		  Sum sumagg = entry.getAggregations().get("my_sum_fieldB");
		  double fieldValue = sumagg.getValue();
		  System.out.println("fieldAValue="+fieldAValue);
		  System.out.println("sumagg="+fieldValue);
	  }*/

     /*
       public void testAggreation(){
		按某个field 和 date group by 并查询另一个filed的sum，时间统计图，时间间隔是1天。
		SELECT
		DATE(create_at), fieldA, SUM(fieldB)
		from table
		group by DATE(create_at), fieldA;
        SearchRequestBuilder searchReq = getClient().prepareSearch("tempindex");
        searchReq.setTypes("tempindex");
        DateHistogramBuilder dhb = AggregationBuilders.dateHistogram("my_datehistogram").field("birthday").interval(DateHistogramInterval.DAY);
        TermsBuilder termsb_fa = AggregationBuilders.terms("my_fieldA").field("home").size(100);
        termsb_fa.subAggregation(AggregationBuilders.sum("my_sum_fieldB").field("height"));
        dhb.subAggregation(termsb_fa);

        searchReq.setQuery(QueryBuilders.matchAllQuery()).addAggregation(dhb);
        SearchResponse searchRes = searchReq.execute().actionGet();
        Histogram dateHist=searchRes.getAggregations().get("my_datehistogram");
        for (Histogram.Bucket dateBucket : dateHist.getBuckets()) {
            //DATE(create_at)
            String create_at = dateBucket.getKey().toString();
            System.out.println("create_at="+create_at);
            Terms fieldATerms = dateBucket.getAggregations().get("my_fieldA");
            for (Terms.Bucket filedABucket : fieldATerms.getBuckets()) {
                //fieldA
                String fieldAValue = filedABucket.getKey().toString();
                System.out.println("home="+fieldAValue);
                //SUM(fieldB)
                Sum sumagg = filedABucket.getAggregations().get("my_sum_fieldB");
                long sumFieldB = (long)sumagg.getValue();

                System.out.println("height="+sumFieldB);
            }
        }

    }
    */


    }

}
