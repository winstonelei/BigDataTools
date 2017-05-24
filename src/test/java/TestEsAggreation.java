import com.github.bigDataTools.es.EsSearchManager;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

/**
 * Created by wistone  on 2017/5/24.
 */
public class TestEsAggreation {

    /*

    public void buildIndexMapping() throws Exception {
		Map<String, Object> settings = new HashMap<>();
		settings.put("number_of_shards", 4);//分片数量
		settings.put("number_of_replicas", 0);//复制数量
		settings.put("refresh_interval", "10s");//刷新时间

		//在本例中主要得注意,ttl及timestamp如何用java ,这些字段的具体含义,请去到es官网查看
		CreateIndexRequestBuilder cib = Es_Utils.client.admin().indices().prepareCreate(Es_Utils.LOGSTASH_YYYY_MM_DD);
		cib.setSettings(settings);

		XContentBuilder mapping = XContentFactory.jsonBuilder()
				.startObject()
				.startObject("we3r")//
				.startObject("_ttl")//有了这个设置,就等于在这个给索引的记录增加了失效时间,
				//ttl的使用地方如在分布式下,web系统用户登录状态的维护.
				.field("enabled", true)//默认的false的
				.field("default", "5m")//默认的失效时间,d/h/m/s 即天/小时/分钟/秒
				.field("store", "yes")
				.field("index", "not_analyzed")
				.endObject()
				.startObject("_timestamp")//这个字段为时间戳字段.即你添加一条索引记录后,自动给该记录增加个时间字段(记录的创建时间),搜索中可以直接搜索该字段.
				.field("enabled", true)
				.field("store", "no")
				.field("index", "not_analyzed")
				.endObject()
				//properties下定义的name等等就是属于我们需要的自定义字段了,相当于数据库中的表字段 ,此处相当于创建数据库表
				.startObject("properties")
				.startObject("@timestamp").field("type", "long").endObject()
				.startObject("name").field("type", "string").field("store", "yes").endObject()
				.startObject("home").field("type", "string").field("index", "not_analyzed").endObject()
				.startObject("now_home").field("type", "string").field("index", "not_analyzed").endObject()
				.startObject("height").field("type", "double").endObject()
				.startObject("age").field("type", "integer").endObject()
				.startObject("birthday").field("type", "date").field("format", "YYYY-MM-dd").endObject()
				.startObject("isRealMen").field("type", "boolean").endObject()
				.startObject("location").field("lat", "double").field("lon", "double").endObject()
				.endObject()
				.endObject()
				.endObject();
		cib.addMapping(Es_Utils.LOGSTASH_YYYY_MM_DD_MAPPING, mapping);
		cib.execute().actionGet();
	}*/

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

        按某个field 和 date group by 并查询另一个filed的sum，时间统计图，时间间隔是1天。
        SELECT
        DATE(create_at), fieldA, SUM(fieldB)
        from table
        group by DATE(create_at), fieldA;

        SearchRequestBuilder searchReq = client.prepareSearch("sample_index");
        searchReq.setTypes("sample_types");
        DateHistogramBuilder dhb = AggregationBuilders.dateHistogram("my_datehistogram").field("create_at").interval(DateHistogram.Interval.days(1));
        TermsBuilder termsb_fa = AggregationBuilders.terms("my_fieldA").field("fieldA").size(100);
        termsb_fa.subAggregation(AggregationBuilders.sum("my_sum_fieldB").field("fieldB"));
        dhb.subAggregation(termsb_fa);

        searchReq.setQuery(QueryBuilders.matchAllQuery()).addAggregation(dhb);
        SearchResponse searchRes = searchReq.execute().actionGet();

        DateHistogram dateHist = searchRes.getAggregations().get("my_datehistogram");
        for (DateHistogram.Bucket dateBucket : dateHist.getBuckets()) {
            //DATE(create_at)
            String create_at = dateentry.getKey();
            Terms fieldATerms = dateBucket.getAggregations().get("my_fieldA");
            for (Terms.Bucket filedABucket : fieldATerms.getBuckets()) {
                //fieldA
                String fieldAValue = filedABucket.getKey();

                //SUM(fieldB)
                Sum sumagg = filedABucket.getAggregations().get("my_sum_fieldB");
                long sumFieldB = (long)sumagg.getValues();
            }
        }
*/


    }

}
