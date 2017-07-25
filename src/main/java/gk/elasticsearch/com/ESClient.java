package gk.elasticsearch.com;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.get.GetResponse;
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
			//注：此处的port修改成9300
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
	
}
