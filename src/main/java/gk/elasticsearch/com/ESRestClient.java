package gk.elasticsearch.com;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * @Description: TODO
 * @Author: GK
 * @Date: 2019/11/1
 */
public class ESRestClient {

    private RestHighLevelClient highLevelClient;

    public ESRestClient() {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost",9200,"http")
                )
        );
        highLevelClient = client;
    }

    public void close() {
        if(null != highLevelClient) {
            try {
                highLevelClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public RestHighLevelClient getClient() {
        return highLevelClient;
    }

}
