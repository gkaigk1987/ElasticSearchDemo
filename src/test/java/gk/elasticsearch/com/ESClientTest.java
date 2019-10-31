package gk.elasticsearch.com;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gk.elasticsearch.com.mapper.ThesisMapper;
import gk.elasticsearch.com.model.Thesis;
import gk.elasticsearch.com.model.ThesisExample;

public class ESClientTest {
	
	private static Logger logger = LoggerFactory.getLogger(ESClientTest.class);
	
	private ESClient esClient;
	
	@Before
	public void before() {
		esClient = new ESClient();
	}
	
	@After
	public void after() {
		esClient.close();
	}
	
	@Test
	public void createIndex() {
		IndicesAdminClient indicesAdminClient = esClient.getAdmin().indices();
		CreateIndexResponse createIndexResponse = indicesAdminClient.prepareCreate("thesis")
					.setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 1)).get();
		System.out.println(createIndexResponse.isAcknowledged());
	}
	
	@Test
	public void putMapping() throws IOException {
		IndicesAdminClient indicesAdminClient = esClient.getAdmin().indices();
		XContentBuilder builder = XContentFactory.jsonBuilder();
		builder = builder.startObject().startObject("properties")
				.startObject("id").field("type", "long").endObject()
				.startObject("thesisCode").field("type", "keyword").endObject()
				.startObject("title").field("type","text").endObject()
				.startObject("author").field("type", "text").endObject()
				.startObject("pages").field("type", "integer").endObject()
				.startObject("schoolCode").field("type", "keyword").endObject()
				.startObject("schoolName").field("type", "keyword").endObject()
				.startObject("year").field("type", "integer").endObject()
				.startObject("lang").field("type", "keyword").endObject()
				.startObject("summary").field("type", "text").endObject()
			.endObject().endObject();
		AcknowledgedResponse putMappingResponse = indicesAdminClient.preparePutMapping("thesis").setType("pqdt_thesis").setSource(builder).get();
		System.out.println(putMappingResponse.isAcknowledged());
	}
	
	/**
	 * 效率差
	 * @throws IOException
	 */
	@Test
	public void insertIndexData() throws IOException {
		Reader resourceAsReader = Resources.getResourceAsReader("SqlMapConfig.xml");
		SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(resourceAsReader);
		SqlSession sqlSession = sqlSessionFactory.openSession();
		
		ThesisMapper thesisMapper = sqlSession.getMapper(ThesisMapper.class);
		List<Thesis> list = thesisMapper.selectByExampleWithBLOBs(new ThesisExample());
		int dataSize = list.size();
		logger.info("获取{}条论文数据",dataSize);
		
		int pages = dataSize / 1000;//总页数
		int mode = dataSize % 1000;//余数
		if(mode > 0) {
			pages += 1;
		}
		int pageSize = 1000;
		for(int i = 0; i < pages; i++) {
			int offset = i * pageSize;
			Map<String, Object> param = new HashMap<>();
			param.put("offset", offset);
			param.put("pageSize", pageSize);
			List<Thesis> pageThesis = thesisMapper.getPageThesis(param);
			pageThesis.stream().forEach(t -> {
				try {
					XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
					esClient.getClient().prepareIndex("thesis", "pqdt_thesis", String.valueOf(t.getId()))
							.setSource(jsonBuilder.startObject()
									.field("id", t.getId())
									.field("thesisCode", t.getThesisCode())
									.field("title", t.getThesisTitle())
									.field("author", t.getThesisAuthor())
									.field("pages", t.getThesisPages())
									.field("schoolCode", t.getThesisSchoolCode())
									.field("schoolName", t.getThesisSchoolName())
									.field("year", t.getThesisYear())
									.field("lang", t.getThesisLang())
									.field("summary", t.getThesisSummary())
									.endObject()
									).get();
//					logger.info("{}次插入索引，相应:{}",i,indexResponse.status());
				} catch (Exception e) {
					logger.error("插入索引数据出错:{}",e.getMessage());
					e.printStackTrace();
				}
			});
			logger.info("{}次插入索引成功",i);
		}
		
		sqlSession.close();
	}
	
	@Test
	public void createIndex1() {
		IndicesAdminClient indicesAdminClient = esClient.getAdmin().indices();
		CreateIndexResponse createIndexResponse = indicesAdminClient.prepareCreate("thesis2")
					.setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 1)).get();
		System.out.println(createIndexResponse.isAcknowledged());
	}
	
	@Test
	public void putMapping1() throws IOException {
		IndicesAdminClient indicesAdminClient = esClient.getAdmin().indices();
		XContentBuilder builder = XContentFactory.jsonBuilder();
		builder = builder.startObject().startObject("properties")
				.startObject("id").field("type", "long").endObject()
				.startObject("thesisCode").field("type", "keyword").endObject()
				.startObject("title").field("type","text").endObject()
				.startObject("author").field("type", "text").endObject()
				.startObject("pages").field("type", "integer").endObject()
				.startObject("schoolCode").field("type", "keyword").endObject()
				.startObject("schoolName").field("type", "keyword").endObject()
				.startObject("year").field("type", "integer").endObject()
				.startObject("lang").field("type", "keyword").endObject()
				.startObject("summary").field("type", "text").endObject()
			.endObject().endObject();
		AcknowledgedResponse putMappingResponse = indicesAdminClient.preparePutMapping("thesis2").setType("pqdt_thesis2").setSource(builder).get();
		System.out.println(putMappingResponse.isAcknowledged());
	}
	
	/**
	 * 效率更高
	 * @throws IOException
	 */
	@Test
	public void insertIndexDataWithBulk() throws IOException {
		Reader resourceAsReader = Resources.getResourceAsReader("SqlMapConfig.xml");
		SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(resourceAsReader);
		SqlSession sqlSession = sqlSessionFactory.openSession();
		ThesisMapper thesisMapper = sqlSession.getMapper(ThesisMapper.class);
		List<Thesis> list = thesisMapper.selectByExampleWithBLOBs(new ThesisExample());
		int dataSize = list.size();
		logger.info("获取{}条论文数据",dataSize);
		
		int pages = dataSize / 10000;//总页数
		int mode = dataSize % 10000;//余数
		if(mode > 0) {
			pages += 1;
		}
		int pageSize = 10000;
		Client client = esClient.getClient();
		for(int i = 0; i < pages; i++) {
			int offset = i * pageSize;
			Map<String, Object> param = new HashMap<>();
			param.put("offset", offset);
			param.put("pageSize", pageSize);
			List<Thesis> pageThesis = thesisMapper.getPageThesis(param);
			BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
			pageThesis.stream().forEach(t -> {
				try {
					XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
					bulkRequestBuilder.add(client.prepareIndex("thesis", "_doc", String.valueOf(t.getId()))
							.setSource(jsonBuilder.startObject()
									.field("id", t.getId())
									.field("thesisCode", t.getThesisCode())
									.field("title", t.getThesisTitle())
									.field("author", t.getThesisAuthor())
//									.field("pages", t.getThesisPages())
//									.field("schoolCode", t.getThesisSchoolCode())
//									.field("schoolName", t.getThesisSchoolName())
									.field("year", t.getThesisYear())
									.field("lang", t.getThesisLang())
									.field("summary", t.getThesisSummary())
									.endObject()
							));
//					logger.info("{}次插入索引，相应:{}",i,indexResponse.status());
				} catch (Exception e) {
					logger.error("插入索引数据出错:{}",e.getMessage());
					e.printStackTrace();
				}
			});
			BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
			logger.info("{}次插入索引成功,返回类型:{}",i,bulkResponse.status());
		}
		sqlSession.close();
	}
	
	@Test
	public void updateIndex() throws IOException, InterruptedException, ExecutionException {
		Client client = esClient.getClient();
		//方式一
//		UpdateRequest updateRequest = new UpdateRequest();
//		updateRequest.index("thesis").type("pqdt_thesis").id("284428");
		//方式二
		UpdateRequest updateRequest = new UpdateRequest("thesis", "pqdt_thesis", "284428");
		XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
		updateRequest.doc(jsonBuilder.startObject().field("pages", 20).endObject());
		UpdateResponse updateResponse = client.update(updateRequest).get();
		logger.info("更新成功：{}",updateResponse.status());
	}
	
	/**
	 * 没有则新增，有则修改
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testUpsert() throws IOException, InterruptedException, ExecutionException {
		XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
		IndexRequest indexRequest = new IndexRequest("thesis", "pqdt_thesis", "1");
		indexRequest.source(jsonBuilder.startObject()
				.field("id", "1")
				.field("thesisCode", "111111")
				.field("title", "my test add title")
				.endObject());
		UpdateRequest updateRequest = new UpdateRequest("thesis", "pqdt_thesis", "1");
		XContentBuilder jsonBuilder2 = XContentFactory.jsonBuilder();
		updateRequest.doc(jsonBuilder2.startObject()
				.field("id", "1")
				.field("thesisCode", "222222")
				.endObject()).upsert(indexRequest);
		Client client = esClient.getClient();
		UpdateResponse updateResponse = client.update(updateRequest).get();
		logger.info("更新或新增完成，返回状态码：{}",updateResponse.status());
	}
	
	@Test
	public void testMget() {
		Client client = esClient.getClient();
		MultiGetRequestBuilder prepareMultiGet = client.prepareMultiGet();
		List<String> ids = new ArrayList<>();
		ids.add("284429");
		ids.add("284430");
		ids.add("284431");
		MultiGetResponse multiGetResponse = prepareMultiGet.add("thesis","pqdt_thesis",ids).get();//该方法有多个重载
		for (MultiGetItemResponse multiGetItemResponse : multiGetResponse) {
			GetResponse getResponse = multiGetItemResponse.getResponse();
			if(getResponse.isExists()) {
				System.out.println(getResponse.getSourceAsString());
			}
		}
	}
	
	/**
	 * 该方法未执行测试
	 */
	@Test
	public void testBulkProcessor() {
		Client client = esClient.getClient();
		BulkProcessor bulkProcessor = BulkProcessor.builder(client, new Listener() {
			//bulk执行前调用
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
				// do something
			}
			//bulk执行后出现失败异常时调用
			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				// do something
			}
			//bulk执行成功后调用
			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
				// do something
			}
		}).setBulkActions(10000)	//每10000次request调用bulk操作，默认1000
		.setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))	//每10MB内容刷新bulk，默认5MB
		.setConcurrentRequests(1)	//同步request数，0表示一个同时只有一个request发出，1表示允许2个request同时发出，默认值是1
		.setFlushInterval(TimeValue.timeValueSeconds(15))	//15秒刷新bulk，而忽略request的数量，默认不设置
		.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMinutes(1), 3))	//设置回退策略，该策略最初等待1分钟，呈指数级增长，最多重试3次，关闭回退策略使用BackoffPolicy.noBackoff()。默认是50ms重试8次
		.build();
//		bulkProcessor.add(new IndexRequest("twitter", "_doc", "1").source(""));
//		bulkProcessor.add(new DeleteRequest("twitter", "_doc", "2"));
		//关闭方式一
//		bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
		//关闭方式二
		bulkProcessor.close();
	}
	
	/**
	 * 官方API文档有错误
	 * new Script(ScriptType type, String lang, String idOrCode, Map<String, Object> params)
	 * 第二个参数是lang即"painless"，官方文档将其写到了第三个参数
	 * 说明：ctx._source.pages = 31，表示更新字段是pages的值为31
	 */
	@Test
	public void testUpdateByQuery() {
		Client client = esClient.getClient();
//		UpdateByQueryRequestBuilder updateByQuery = UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
		UpdateByQueryRequestBuilder updateByQuery = new UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE);
		updateByQuery.source("thesis").filter(QueryBuilders.matchQuery("year", 2019)).script(
				new Script(ScriptType.INLINE, "painless", "ctx._source.pages = 31", Collections.emptyMap()));
		BulkByScrollResponse bulkByScrollResponse = updateByQuery.get();
		for (Failure failure :  bulkByScrollResponse.getBulkFailures()) {
			System.out.println(failure.getMessage());
		}
	}
	
	/**
	 * match检索是分词检索，不需要全词匹配
	 */
	@Test
	public void testMatchQuery() {
		MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("title", "developing");
		Client client = esClient.getClient();
		SearchResponse searchResponse = client.prepareSearch("thesis").setQuery(matchQuery).setSize(10).get();
		SearchHits hits = searchResponse.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
		}
	}
	
	/**
	 * 分页测试
	 */
	@Test
	public void testPagination() {
		MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("title", "developing");
		Client client = esClient.getClient();
		SearchResponse searchResponse = client.prepareSearch("thesis").setQuery(matchQuery)
				.setFrom(0)	//从哪条记录开始 相当于offset
				.setSize(10)//查询多少条记录 相当于pagesize
				.get();
		SearchHits hits = searchResponse.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
		}
	}
	
	/**
	 * term查询是不分词的，需要全词匹配
	 */
	@Test
	public void testTermQuery() {
//		TermQueryBuilder termQuery = QueryBuilders.termQuery("title", "Question");	//查不到值，因为需要全词匹配
		TermQueryBuilder termQuery = QueryBuilders.termQuery("year", 2019);	
		Client client = esClient.getClient();
		SearchResponse searchResponse = client.prepareSearch("thesis").setQuery(termQuery).setSize(20).get();
		SearchHits hits = searchResponse.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
		}
	}
	
	@Test
	public void testRangQuery() {
		RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("year").from(2016).to(2019);
		Client client = esClient.getClient();
		SearchResponse searchResponse = client.prepareSearch("thesis").setQuery(rangeQuery).setSize(20).get();
		SearchHits hits = searchResponse.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
		}
	}
	
	/**
	 * 待完善
	 */
	@Test
	public void testHighlighter() {
		Client client = esClient.getClient();
		HighlightBuilder highlightBuilder = new HighlightBuilder();
		//高亮显示规则
		highlightBuilder.preTags("<span style='color:red'>");
		highlightBuilder.postTags("</span>");
		//高亮字段
		highlightBuilder.field("title");
		
		MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("title", "developing");
		SearchResponse searchResponse = client.prepareSearch("thesis").setQuery(matchQuery).highlighter(highlightBuilder).setSize(10).get();
		SearchHits hits = searchResponse.getHits();
		for (SearchHit searchHit : hits) {
			Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
			HighlightField highlightField = highlightFields.get("title");
			Text[] texts = highlightField.getFragments();
			System.out.println(texts[0].toString());
			System.out.println(searchHit.getSourceAsString());
		}
	}
	
	/**
	 * bucket聚合
	 */
	@Test
	public void testAggregations() {
		Client client = esClient.getClient();
		SearchResponse searchResponse = client.prepareSearch("thesis")
				.addAggregation(AggregationBuilders.terms("by_lang").size(100).field("lang")).get();
		Terms aggregation = searchResponse.getAggregations().get("by_lang");
		List<? extends Bucket> buckets = aggregation.getBuckets();
		for (Bucket bucket : buckets) {
			System.out.println(bucket.getKeyAsString() + ":" + bucket.getDocCount());
		}
	}
	
	/**
	 * bucket子聚合
	 */
	@Test
	public void testSubAggregations() {
		Client client = esClient.getClient();
		SearchResponse searchResponse = client.prepareSearch("thesis")
				.addAggregation(AggregationBuilders.terms("by_lang").field("lang").size(50)
						.subAggregation(AggregationBuilders.terms("by_year").field("year").size(20)))
				.get();
		Terms aggregation = searchResponse.getAggregations().get("by_lang");
		List<? extends Bucket> buckets = aggregation.getBuckets();
		for (Bucket bucket : buckets) {
			String keyAsString = bucket.getKeyAsString();
			long docCount = bucket.getDocCount();
			System.out.println(keyAsString + ":" + docCount);
			Terms aggregation2 = bucket.getAggregations().get("by_year");
			List<? extends Bucket> buckets2 = aggregation2.getBuckets();
			for (Bucket bucket2 : buckets2) {
				String keyAsString2 = bucket2.getKeyAsString();
				long docCount2 = bucket2.getDocCount();
				System.out.println("		" + keyAsString2 + ":" + docCount2);
			}
		}
	}
	
	@Test
	public void testMetricsAggregations() {
		Client client = esClient.getClient();
		//min agg
		MinAggregationBuilder minAggregationBuilder = AggregationBuilders.min("minAgg").field("year");
		SearchResponse searchResponse = client.prepareSearch("thesis").addAggregation(minAggregationBuilder).get();
		Min aggregation = searchResponse.getAggregations().get("minAgg");
		double value = aggregation.getValue();
		System.out.println("Min Aggregation: " + value);
		
		//max agg
		MaxAggregationBuilder maxAggregationBuilder = AggregationBuilders.max("maxAgg").field("year");
		searchResponse = client.prepareSearch("thesis").addAggregation(maxAggregationBuilder).get();
		Max maxAggregation = searchResponse.getAggregations().get("maxAgg");
		double maxValue = maxAggregation.getValue();
		System.out.println("Max Aggregation: " + maxValue);
		
		//sum agg
		
		//avg agg
		
		//stats agg 获取的Stats对象中包含min,max,avg,sum,count
		
		//extended stat agg
		
		//value count agg
		
		//percentiles agg
		
		//percentile ranks agg
		
		//cardinality(基数) agg 
		
		//geo bounds agg
		
		//top hits agg
	}
	
	@Test
	public void bucketAggregation() {
		//global agg
		
		//filter agg
		
		//filters agg
		
		//missing agg 索引中缺少某个字段的agg
		
		//nested agg
		
		//reverse nested agg
		
		//children agg
		
		//terms agg 常用 可以加上order对键或值进行排序
		
		//range agg
		
		//date range agg
		
		//Ip range agg
		
		//histogram agg
		
		//date histogram agg
		
		//geo distance agg
		
	}
}
