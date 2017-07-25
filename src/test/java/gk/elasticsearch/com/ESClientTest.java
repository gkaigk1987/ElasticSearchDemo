package gk.elasticsearch.com;

import org.junit.Before;
import org.junit.Test;

public class ESClientTest {
	
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

}
