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
	
	@Test
	public void test02() {
		GktestIndex index = new GktestIndex();
		index.setId(5L);
		index.setName("南京");
		index.setAge(5000);
		esClient.index(index);
	}

}
