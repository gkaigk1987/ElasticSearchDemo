package gk.elasticsearch.com;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ESClient {
	
	private Client client;
	
	@SuppressWarnings({ "resource"})
	public ESClient() {
		try {
			//注：此处的port修改成9300，如果集群名不是默认的elasticsearch，则需要设置Settings
			Settings settings = Settings.builder()
			        .put("cluster.name", "elasticsearch").build();
			client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"),9300));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public void close() {
		client.close();
	}
	
	public Client getClient() {
		return client;
	}
	
	public AdminClient getAdmin() {
		AdminClient adminClient = client.admin();
		return adminClient;
	}
	
	
}
