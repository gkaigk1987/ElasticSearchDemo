package gk.elasticsearch.com;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.alibaba.fastjson.JSONObject;

public class ESClient {
	
	private Client client;
	
	@SuppressWarnings({ "resource", "unchecked" })
	public ESClient() {
		try {
			//注：此处的port修改成9300，如果集群名不是默认的elasticsearch，则需要设置Settings
//			Settings settings = Settings.builder()
//			        .put("cluster.name", "myClusterName").build();
//			TransportClient client = new PreBuiltTransportClient(settings);
			client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.200"),9300));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public void close() {
		client.close();
	}
	
	public String get() {
		GetResponse response = client.prepareGet("gktest", "gaokai", "1").get();
		return JSONObject.toJSONString(response.getSource());
	}
	
	@SuppressWarnings("deprecation")
	public void index(GktestIndex index) {
		IndexResponse indexResponse = client.prepareIndex("gktest", "gaokai").setSource(JSONObject.toJSONString(index)).get();
		System.out.println(indexResponse.getId());
	}
	
	public void index2(GktestIndex index) {
//		XContentBuilder builder = XContentFactory.jsonBuilder().startObject().
		IndexResponse indexResponse = client.prepareIndex("gktest", "gaokai").setSource(index).get();
		System.out.println(indexResponse.getId());
	}
	
	public void index3() {
		
	}
	
}
