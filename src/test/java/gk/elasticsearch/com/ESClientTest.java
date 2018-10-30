package gk.elasticsearch.com;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
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
		PutMappingResponse putMappingResponse = indicesAdminClient.preparePutMapping("thesis").setType("pqdt_thesis").setSource(builder).get();
		System.out.println(putMappingResponse.isAcknowledged());
	}
	
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
		PutMappingResponse putMappingResponse = indicesAdminClient.preparePutMapping("thesis2").setType("pqdt_thesis2").setSource(builder).get();
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
					bulkRequestBuilder.add(client.prepareIndex("thesis2", "pqdt_thesis2", String.valueOf(t.getId()))
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
	
}
