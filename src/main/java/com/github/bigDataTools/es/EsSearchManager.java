package com.github.bigDataTools.es;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;

import com.github.bigDataTools.hbase.HbaseManager;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;

/**
 * @author  winstonelei
 *
 * operate for es
 *
 */
public class EsSearchManager {

	private final Logger LOG = LoggerFactory.getLogger(EsSearchManager.class);

	public Client client;

	private void init() throws Exception {
		client = EsClient.getClient();
	}

	private Client getClient()  {
		try{
			if(client==null){
				init();
			}
		}catch (Exception e){
			LOG.error(e.getMessage());
		}
		return client;
	}

	public void setClient(Client client) {
		this.client = client;
	}

	private static  EsSearchManager esSearchManager;

	private EsSearchManager(){
		getClient();
	}

	public static EsSearchManager getInstance(){
		if(null == esSearchManager ){
			synchronized (EsSearchManager.class){
				esSearchManager = new EsSearchManager();
			}
		}
		return  esSearchManager;
	}

	/**
	 * 根据索引名称判断索引是否存在
	 * @param indexName
	 * @return
	 * @throws NumberFormatException
	 * @throws UnknownHostException
     */
	public Boolean existsIndex(String indexName) throws Exception {
		IndicesExistsResponse response = getClient().admin().indices()
				.prepareExists(indexName).execute().actionGet();
		return response.isExists();
	}

	/**
	 * 根据索引名称删除索引
	 * @param indexName
	 * @return
	 * @throws NumberFormatException
	 * @throws UnknownHostException
     */
	public Boolean deleteIndex(String indexName) throws Exception {
		boolean flag = true;
		IndicesExistsResponse response = getClient().admin().indices()
				.prepareExists(indexName).execute().actionGet();
		if (response.isExists()) {
			DeleteIndexResponse response2 = getClient().admin().indices()
					.prepareDelete(indexName).execute().actionGet();
			flag = response2.isAcknowledged();
		}
		return flag;
	}


	/**
	 * 根据索引名称构建索引
	 * @param indexName
	 * @return
	 * @throws Exception
     */
	public Boolean buildIndex(String indexName) throws Exception {
		IndicesExistsResponse response = getClient().admin().indices()
				.prepareExists(indexName).execute().actionGet();
		Boolean flag = true;
		ResourceBundle rb = ResourceBundle.getBundle("commons");
		String replicas = rb.getString("replicas");
		String shards = rb.getString("shards");
		String refreshInterval = rb.getString("refreshInterval");
		if (!response.isExists()) { //需要将配置放置到配置文件中
			Settings settings = Settings.builder()
					.put("number_of_replicas", Integer.parseInt(replicas))
					.put("number_of_shards", Integer.parseInt(shards))
					.put("index.translog.flush_threshold_ops", 10000000)
					.put("refresh_interval", refreshInterval)
					.put("index.codec", "best_compression").build();
			CreateIndexResponse createIndxeResponse = getClient().admin().indices()
					.prepareCreate(indexName).setSettings(settings).execute()
					.actionGet();
			flag = createIndxeResponse.isAcknowledged();
			LOG.info("返回值" + flag);
		}
		return flag;
	}

	/**
	 * 根据索引和类型创建索引
	 * @param indexName
	 * @param type
	 * @return
	 * @throws Exception
     */
	public Boolean buildIndex(String indexName,String type) throws Exception {
		IndicesExistsResponse response = getClient().admin().indices()
				.prepareExists(indexName,type).execute().actionGet();
		Boolean flag = true;
		ResourceBundle rb = ResourceBundle.getBundle("commons");
		String replicas = rb.getString("replicas");
		String shards = rb.getString("shards");
		String refreshInterval = rb.getString("refreshInterval");
		if (!response.isExists()) { //需要将配置放置到配置文件中
			Settings settings = Settings.builder()
					.put("number_of_replicas", Integer.parseInt(replicas))
					.put("number_of_shards", Integer.parseInt(shards))
					.put("index.translog.flush_threshold_ops", 10000000)
					.put("refresh_interval", refreshInterval)
					.put("index.codec", "best_compression").build();
			CreateIndexResponse createIndxeResponse = getClient().admin().indices()
					.prepareCreate(indexName).setSettings(settings).addMapping(type).execute()
					.actionGet();
			flag = createIndxeResponse.isAcknowledged();
			LOG.info("返回值" + flag);
		}
		return flag;
	}

	/**
	 * 创建索引mapping
	 * @param indexName
	 * @param type
	 * @param mapping
	 * @return
     * @throws Exception
     */
	public Boolean buildIndexWithMapping(String indexName,String type,XContentBuilder mapping) throws Exception {
		IndicesExistsResponse response = getClient().admin().indices()
				.prepareExists(indexName,type).execute().actionGet();
		Boolean flag = true;
		ResourceBundle rb = ResourceBundle.getBundle("commons");
		String replicas = rb.getString("replicas");
		String shards = rb.getString("shards");
		String refreshInterval = rb.getString("refreshInterval");
		if (!response.isExists()) { //需要将配置放置到配置文件中
			Settings settings = Settings.builder()
					.put("number_of_replicas", Integer.parseInt(replicas))
					.put("number_of_shards", Integer.parseInt(shards))
					.put("index.translog.flush_threshold_ops", 10000000)
					.put("refresh_interval", refreshInterval)
					.put("index.codec", "best_compression").build();

			CreateIndexResponse createIndxeResponse = getClient().admin().indices()
					.prepareCreate(indexName).setSettings(settings).addMapping(type,mapping).execute()
					.actionGet();
			flag = createIndxeResponse.isAcknowledged();
			LOG.info("返回值" + flag);
		}
		return flag;
	}


	/**
	 * 构建索引根据字段映射
	 * @param indexName
	 * @param typeName
	 * @param fieldInfos
	 * @param excludeFields
	 * @throws Exception
	 */
	public void buildIndexWithFields(String indexName,
									 String typeName,
									 Map<String, String> fieldInfos,
									 List<String> excludeFields
	) throws Exception {

		IndicesExistsResponse response = getClient().admin().indices()
				.prepareExists(indexName).execute().actionGet();

		if (!response.isExists()) { //需要将配置放置到配置文件中

			if (StringUtils.isBlank(typeName) || fieldInfos == null
					|| fieldInfos.isEmpty()) {
				return;
			}
			// 创建字段
			JSONObject mappingJson = new JSONObject();

			// _all
			JSONObject allProperties = new JSONObject();
			allProperties.put("enabled", false);
			mappingJson.put("_all", allProperties);

			// _source
			JSONObject sourceProperties = new JSONObject();
			sourceProperties.put("excludes", excludeFields);
			//	sourceProperties.put("enabled", false);
			mappingJson.put("_source", sourceProperties);

			// _time
			JSONObject timeProperties = new JSONObject();
			timeProperties.put("enabled", "true");
			mappingJson.put("_timestamp", timeProperties);

			JSONObject tableInfos = new JSONObject();
			IndexFieldBuilder ib = new IndexFieldBuilder();
			Iterator<Map.Entry<String, String>> it = fieldInfos.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, String> entry = it.next();
				String name = entry.getKey();
				String type = entry.getValue();
				tableInfos.put(name, ib.buildFieldMapping(type, name));
			}
			mappingJson.put("properties", tableInfos);

			CreateIndexResponse createIndexResponse = getClient().admin().indices()
					.prepareCreate(indexName).addMapping(typeName).setSource(mappingJson.toString()).execute()
					.actionGet();

			if (createIndexResponse.isAcknowledged()) {
				LOG.info("创建es索引表成功:" + typeName);
			}else{
				LOG.error("创建es索引表失败!" + typeName);
			}

		}

	}
	/**
	 * 构建索引数据
	 * @param indexName
	 * @param type
	 * @param docId
	 * @param json
	 * @throws NumberFormatException
	 * @throws UnknownHostException
     */
	public void buildDocumentWithDocId(String indexName, String type, String docId, String json) throws Exception {
		getClient().prepareIndex(indexName, type, docId).setSource(json).execute()
				.actionGet();
	}

	/**
	 * 创建单条索引
	 * @param indexName
	 * @param type
	 * @param json
	 * @throws Exception
     */
	public void buildDocument(String indexName, String type, String json) throws Exception {
		getClient().prepareIndex(indexName, type).setSource(json).execute()
				.actionGet();
	}
	/**
	 * 构造list集合索引数据
	 * @param indexName
	 * @param type
	 * @param list
     */
	public  void buildList2Documents(String indexName, String type, List<Map<String,Object>> list) throws  Exception{
		BulkRequestBuilder bulkRequest = getClient().prepareBulk();
		for(Map<String,Object> map : list){
			bulkRequest.add(getClient().prepareIndex(indexName, type)
					.setSource(this.generateJson(map)));
		}
		BulkResponse bulkIndexResponse = bulkRequest.execute().actionGet();
		if (bulkIndexResponse.hasFailures()) {
			LOG.error(bulkIndexResponse.buildFailureMessage());
		}
	}

	/**
	 * 根据索引Id删除文档
	 * @param indexName
	 * @param type
	 * @param docId
	 * @throws NumberFormatException
	 * @throws UnknownHostException
     */
	public void deleteDocument(String indexName, String type, String docId)
			throws Exception {
		getClient().prepareDelete(indexName, type, docId).execute().actionGet();
	}

	/**
	 * term 查询（在查询的时候不分词，主要针对 人名 地名等特殊的词语）
	 * @param keywords
	 * @param types
	 * @param fieldnames
	 * @param pagenum
	 * @param pagesize
	 * @throws Exception
	 */
	public PageEntity<JSONObject> queryWithTerm(List<String> keywords,
			List<String> indexs, List<String> types, List<String> fieldnames,
			List<String> dateFieldnames, List<String> allColumns,
			Long startTime, Long endTime, int pagenum, int pagesize)
			throws Exception {
		BoolQueryBuilder qb = buildTermQuery(keywords, types, fieldnames,
				dateFieldnames, startTime, endTime);
		if (qb == null) {
			LOG.info("queryTerm() QueryBuilder == null");
			return new PageEntity<JSONObject>();
		}
		LOG.info("query begin");
		long begin = System.currentTimeMillis();
		PageEntity<JSONObject> result = execute(qb, fieldnames, allColumns,
				indexs, types, pagenum, pagesize);
		long end = System.currentTimeMillis();
		LOG.info("query end cost:[{}]ms", end - begin);

		return result;
	}

	/**
	 * 全文检索查询,多条件类型查询
	 * @param keywords
	 * @param indexs
	 * @param types
	 * @param fieldNames
	 * @param allColumns
	 * @param pagenum
	 * @param pagesize
     * @return
     * @throws Exception
     */
	public PageEntity<JSONObject> queryFulltext(List<String> keywords,
			List<String> indexs, List<String> types, List<String> fieldNames,
			List<String> allColumns, int pagenum, int pagesize)
			throws Exception {
		BoolQueryBuilder qb = buildFullText(keywords, types, fieldNames);
		if (qb == null) {
			LOG.info("queryFull Text  == null");
			return null;
		}
		LOG.info("Fulltext begin");
		long begin = System.currentTimeMillis();
		PageEntity<JSONObject> result = execute(qb, fieldNames,allColumns, indexs, types,
				 pagenum, pagesize);
		long end = System.currentTimeMillis();
		LOG.info("query Fulltext end cost:[{}]ms", end - begin);
		return result;
	}


	/**
	 * 构造term 查找实现精确查找
	 * @param keywords
	 * @param types
	 * @param fieldNames
	 * @param dateFieldNames
	 * @param startTime
	 * @param endTime
	 * @return
	 * @throws NumberFormatException
	 * @throws UnknownHostException
     * @throws JSONException
     */
	private BoolQueryBuilder buildTermQuery(List<String> keywords,
			List<String> types, List<String> fieldNames,
			List<String> dateFieldNames, Long startTime, Long endTime)
			throws NumberFormatException, UnknownHostException, JSONException {
		BoolQueryBuilder qb = null;
		if (((keywords != null && keywords.size() > 0) || (dateFieldNames != null && dateFieldNames
				.size() > 0))
				&& fieldNames != null
				&& fieldNames.size() > 0
				&& types != null && types.size() > 0) {
			qb = QueryBuilders.boolQuery();
			if (keywords != null) {
				for (String word : keywords) {
					String lowWord = word.toLowerCase();
					String[] words = lowWord.split(" ");
					BoolQueryBuilder mustQb = QueryBuilders.boolQuery();
					for (String field : fieldNames) {
						QueryBuilder shouldCondition = QueryBuilders
								.termsQuery(field, words);
						mustQb.should(shouldCondition);
					}
					qb.must(mustQb);
				}
			}

			if (dateFieldNames != null) {
				for (String field : dateFieldNames) {
					RangeQueryBuilder rangeQb = QueryBuilders.rangeQuery(field);
					if (startTime != null) {
						rangeQb.gt(startTime).includeLower(true);
					}
					if (endTime != null) {
						rangeQb.lt(endTime).includeUpper(true);
					}
					qb.must(rangeQb);
				}
			}
		}
		return qb;
	}

	/**
	 * 构造多字段 全文搜索查询条件
	 * @param keywords
	 * @param types
	 * @param fieldNames
	 * @return
     * @throws Exception
     */
	private BoolQueryBuilder buildFullText(List<String> keywords,
			List<String> types, List<String> fieldNames)
			throws Exception {
		BoolQueryBuilder qb = null;
		if (keywords != null && keywords.size() > 0 && fieldNames != null
				&& fieldNames.size() > 0 && types != null && types.size() > 0) {
			qb = QueryBuilders.boolQuery();
			for (String keyword : keywords) {
				QueryBuilder mustCondition = QueryBuilders.multiMatchQuery(
						keyword, fieldNames.toArray(new String[0]));
				qb.must(mustCondition);
			}
		}
		return qb;
	}


	/**
	 * 聚合分组查询
	 * @param keywords
	 * @param indexs
	 * @param types
	 * @param fieldNames
	 * @return
     * @throws Exception
     */
	public List<Bucket> queryAggByType(List<String> keywords,
									   List<String> indexs, List<String> types,
									   List<String> fieldNames) throws Exception {
		List<Bucket> bucketlist = Lists.newArrayList();
		BoolQueryBuilder qb = buildTermQuery(keywords, types, fieldNames, null,
				null, null);
		if (qb == null) {
			LOG.info("query agg QueryBuilder == null");
			return null;
		}
		// 根据_type分组聚合
		TermsAggregationBuilder aggregation = AggregationBuilders
				.terms("agg_group").field("_type");

		String[] typeArry = types.toArray(new String[0]);
		String[] indexArry = null;
		if (indexs != null) {
			indexArry = indexs.toArray(new String[0]);
		}

		long begin = System.currentTimeMillis();
		SearchResponse response = null;
		try {
			response = getClient().prepareSearch(indexArry).setTypes(typeArry)
					.setQuery(qb).addAggregation(aggregation).setSize(0)
					// .setExplain(true)// 设置是否按查询匹配度排序
					.execute().actionGet();
		} catch (Exception e) {
			LOG.error("执行搜索引擎查询失败!", e);
			throw e;
		}
		if (response == null) {
			return bucketlist;
		}
		long end = System.currentTimeMillis();

		LOG.info("query agg  cost:[{}]ms", end - begin);

		Terms agg = response.getAggregations().get("agg_group");
		// For each entry
		bucketlist = agg.getBuckets();

		LOG.info("queryAggByType() Bucket-size:[{}]", bucketlist.size());

		return bucketlist;
	}


	/**
	 * es query 实现es分页查询
	 * @param qb
	 * @param fieldnames
	 * @param allColumns
	 * @param indexs
	 * @param types
	 * @param pagenum
	 * @param pagesize
     * @return
     * @throws Exception
     */
	private PageEntity<JSONObject> execute(QueryBuilder qb,
			List<String> fieldnames, List<String> allColumns,
			List<String> indexs, List<String> types, int pagenum, int pagesize)
			throws Exception {
		PageEntity<JSONObject> page = new PageEntity<JSONObject>();
		String[] typeArry = types.toArray(new String[0]);
		String[] indexArry = indexs.toArray(new String[0]);
		int startnum = (pagenum - 1) * pagesize;
		SearchResponse response = null;
		try {
			response = getClient().prepareSearch(indexArry).setTypes(typeArry)
					.setQuery(qb).setFrom(startnum).setSize(pagesize)
					.execute().actionGet();
		} catch (Exception e) {
			LOG.error("query error", e);
			throw e;
		}
		if (response == null) {
			return page;
		}
		SearchHits hits = response.getHits();
		LOG.info("execute hit:" + hits.totalHits());
		List<JSONObject> resultString = new ArrayList<JSONObject>();
		if (null != hits && hits.totalHits() > 0) {
			for (SearchHit hit : hits) {
				JSONObject obj = new JSONObject();
				if (allColumns != null && allColumns.size() > 0) {
					fieldnames = allColumns;
				}
				for (String str : fieldnames) {
					String docId = hit.getId();
					LOG.info(docId);
					obj.put(str, hit.getSource().get(str));
				}
				resultString.add(obj);
			}
		}
		page.setContents(resultString);
		page.setPageSize(pagesize);
		page.setCurrentPageNo(pagenum);
		page.setTotalCount(hits.totalHits());
		return page;
	}

	/**
	 * 实现文档的高亮
	 * @param tempString
	 * @param keywords
     * @return
     */
	public static String highLine(String tempString, List<String> keywords) {
		if (keywords == null || tempString == null) {
			return tempString;
		}
		for (String word : keywords) {
			String[] words = word.split(" ");
			for (String str : words) {
				String replacement = "<font color='red'>" + str + "</font>";
				tempString = tempString.replaceAll(str, replacement);
			}
		}
		return tempString;
	}



	/**
	 * 将map对象转换为json字符串
	 * @param map
	 * @return
     */
	private String generateJson(Map<String,Object> map) {
		String json = "";
		try {
			XContentBuilder contentBuilder = XContentFactory.jsonBuilder()
					.startObject();

			Set<Map.Entry<String,Object>> set =map.entrySet();

			Iterator<Map.Entry<String,Object>> iter = set.iterator();

			while(iter.hasNext()){
				Map.Entry entry = iter.next();
				contentBuilder.field(entry.getKey()+"",entry.getValue() + "");
			}

			json = contentBuilder.endObject().string();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return json;
	}



}
