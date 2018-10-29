package gk.elasticsearch.com;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESClientTest {
	
	private static Logger logger = LoggerFactory.getLogger(ESClientTest.class);
	
	private ESClient esClient;
	
	@Before
	public void before() {
		esClient = new ESClient();
	}
	
	@Test
	public void test01() {
		String string = esClient.get();
		System.out.println(string);
	}
	
	@Test
	public void test02() {
//		GktestIndex index = new GktestIndex();
//		index.setId(5L);
//		index.setName("南京");
//		index.setAge(5000);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("id", 5L);
		map.put("name", "南京");
		map.put("age", 5000);
		esClient.index(map);
	}
	
	@Test
	public void test03() {
		GktestIndex index = new GktestIndex();
		index.setId(4L);
		index.setName("江苏");
		index.setAge(5001);
		esClient.index2(index);
	}
	
	

}
