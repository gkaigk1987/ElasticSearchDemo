package gk.elasticsearch.com;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 使用版本 ES7.X
 * @Description: TODO
 * @Author: GK
 * @Date: 2019/11/1
 */
public class ESRestClientTest {

    private static Logger logger = LoggerFactory.getLogger(ESRestClientTest.class);

    private ESRestClient esRestClient;

    @Before
    public void before() {
        esRestClient = new ESRestClient();
    }

    @After
    public void after() {
        esRestClient.close();
    }

    /**
     * 测试创建 settings 和 mapping
     * @throws IOException
     */
    @Test
    public void createIndexAndMapping() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("test_index");  //必须小写
        request.settings(Settings.builder()
            .put("index.number_of_shards",1)
            .put("index.number_of_replicas",1)
        );
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder = builder.startObject().startObject("properties")
                    .startObject("id").field("type","long").endObject()
                    .startObject("name").field("type","text").startObject("fields")
                        .startObject("keyword").field("type","keyword")
                        .endObject().endObject().endObject()
                    .startObject("age").field("type","integer").endObject()
                    .startObject("desc").field("type","text").field("analyzer","ik_max_word").endObject()
                .endObject().endObject();
        request.mapping(builder);
        request.setTimeout(TimeValue.timeValueMinutes(1));  //等待所有节点将索引创建确认为TimeValue的超时,可选设置，非必须
        CreateIndexResponse createIndexResponse = esRestClient.getClient().indices().create(request, RequestOptions.DEFAULT);
        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println("===================" + acknowledged + "===============");
    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-document-index.html
     * 插入单个doc
     * 没有则新增，有则修改，修改需特别注意，此修改逻辑是删除原先的再添加进去
     * 即如果修改时某个字段值没带上，则原先索引的该字段就会被置空，即没有该字段的值
     */
    @Test
    public void indexApi() {
        IndexRequest indexRequest = new IndexRequest("test_index");
        Map<String,Object> map = new HashMap<>();
        map.put("id",1);
        map.put("name","赵冬晋");
        map.put("age",25);
        map.put("desc","赵冬晋就是一个逗逼");
        indexRequest.id("1").source(map);
        //下面可以定义以何种形式插入，此处定义的是新增
//      indexRequest.id("1").source(map).opType(DocWriteRequest.OpType.CREATE);
        indexRequest.timeout(TimeValue.timeValueSeconds(1));    //等待主分区可用的超时时间，可选设置，非必须
        try {
            //同步执行
            IndexResponse response = esRestClient.getClient().index(indexRequest, RequestOptions.DEFAULT);
            //异步执行是以下方法，需要定义回调Listener
            //esRestClient.getClient().indexAsync();
            String index = response.getIndex();
            System.out.println("===============index: " + index + "====================");
            String id = response.getId();
            System.out.println("===============id: " + id + "====================");
            RestStatus status = response.status();
            System.out.println("===============status: " + status + "====================");
            long version = response.getVersion();
            System.out.println("===============version: " + version + "====================");
            if(response.getResult() == DocWriteResponse.Result.CREATED) {
                System.out.println("==========================DOC被新增==========================");
            }else if(response.getResult() == DocWriteResponse.Result.UPDATED) {
                System.out.println("==========================DOC被修改==========================");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ElasticsearchException e) {
            //未能正确返回
            if(e.status() == RestStatus.CONFLICT) {
                //
            }
        }
    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-document-get.html
     * 获取单个doc
     * @throws IOException
     */
    @Test
    public void getApi() throws IOException {
        GetRequest getRequest = new GetRequest("test_index","4");
//        getRequest.routing("route1");   //路由到哪个shard(分片)
//        getRequest.preference("_primary");  //偏好设置
//        getRequest.version(1);    //获取指定版本的数据
        try {
            //同步
            GetResponse response = esRestClient.getClient().get(getRequest, RequestOptions.DEFAULT);
            //异步
//              esRestClient.getClient().getAsync();
            String index = response.getIndex();
            System.out.println("===============index: " + index + "====================");
            String id = response.getId();
            System.out.println("===============id: " + id + "====================");
            boolean exists = response.isExists();
            if(exists) {
                long version = response.getVersion();
                System.out.println("===============version: " + version + "====================");
                String sourceAsString = response.getSourceAsString();
                System.out.println(sourceAsString);
                Map<String, Object> source = response.getSourceAsMap();
                System.out.println(source);
            }
        }catch (ElasticsearchException e) {
            if(e.status() == RestStatus.NOT_FOUND) {
                //如果是一个不存在的Index，则会进入该异常
                System.out.println("异常，索引中不存在");
            }
        }
    }

    /**
     * 查找某个doc是否存在，与上面的getApi几乎相同，但如果是一个不存在
     * 的index不会抛出异常
     */
    @Test
    public void existApi() throws IOException {
        GetRequest getRequest = new GetRequest("test_index","1");
        boolean exists = esRestClient.getClient().exists(getRequest, RequestOptions.DEFAULT);
        System.out.println("======================"+ exists +"=========================");
    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-document-delete.html
     * 删除索引指定doc
     * @throws IOException
     */
    @Test
    public void deleteApi() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("test_index","1");
//        deleteRequest.timeout(TimeValue.timeValueSeconds(2));   //等待主分区可用的超时时间，可选设置，非必须
        DeleteResponse response = esRestClient.getClient().delete(deleteRequest, RequestOptions.DEFAULT);
        String index = response.getIndex(); //test_index
        String id = response.getId();   //1
        RestStatus status = response.status();
        System.out.println("===================="+ status +"==================");
        DocWriteResponse.Result result = response.getResult();
        if(result == DocWriteResponse.Result.NOT_FOUND) {
            //索引中不存在
            System.out.println("索引中不存在需要删除的DOC");
        }else {
            System.out.println("删除索引中指定的DOC成功");
        }
    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-document-update.html
     * 更新索引中指定的doc
     * @throws IOException
     */
    @Test
    public void updateApi() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("test_index","1");
        Map<String,Object> map = new HashMap<>();
        map.put("name","赵冬晋");
        updateRequest.doc(map);
//        updateRequest.timeout(TimeValue.timeValueSeconds(2));
        try {
            updateRequest.fetchSource(true);    // ① 更新后返回值
            UpdateResponse response = esRestClient.getClient().update(updateRequest, RequestOptions.DEFAULT);
            //未设置 ① ，则下面的getGetResult为null
            GetResult getResult = response.getGetResult();
            if(getResult.isExists()) {
                System.out.println("更新成功");
                String sourceAsString = getResult.sourceAsString();
                System.out.println(sourceAsString);
                Map<String, Object> sourceAsMap = getResult.sourceAsMap();
                System.out.println(sourceAsMap);
            }else {
                // 不存在的操作
            }
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                System.out.println("创建了索引");
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                System.out.println("更新了索引");
            } else if (response.getResult() == DocWriteResponse.Result.DELETED) {
                System.out.println("删除了索引");
            } else if (response.getResult() == DocWriteResponse.Result.NOOP) {
                System.out.println("未操作索引");
            }
        }catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                //id未找到会进入该异常
                System.out.println("未找到需要操作的DOC");
            }
        }

    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-document-bulk.html
     * 批量操作doc
     */
    @Test
    public void bulkApi() {

    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-document-multi-get.html
     */
    @Test
    public void multiGetApi() {

    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-document-reindex.html
     */
    @Test
    public void reindexApi(){

    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-document-update-by-query.html
     */
    @Test
    public void updateByQueryApi() {

    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-document-delete-by-query.html
     */
    @Test
    public void deleteByQueryApi() {

    }

    @Test
    public void basicSearch() throws IOException {
//        SearchRequest searchRequest = new SearchRequest();    //表示查询所有index
        SearchRequest searchRequest = new SearchRequest("thesis");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.trackTotalHits(true);   //设置追踪hit总数，否则默认最多为10000
//        searchSourceBuilder.trackTotalHitsUpTo();
        SimpleQueryStringBuilder simpleQueryStringBuilder = QueryBuilders.simpleQueryStringQuery("english");
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
//        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery();
//        MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery();
        searchSourceBuilder.query(simpleQueryStringBuilder);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        searchSourceBuilder.timeout(TimeValue.timeValueSeconds(10));    //检索超时时间
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = esRestClient.getClient().search(searchRequest, RequestOptions.DEFAULT);
        RestStatus status = response.status();
        System.out.println("status：" + status);
        TimeValue took = response.getTook();
        System.out.println("took：" + took.getMillis());
        SearchHits hits = response.getHits();
        TotalHits totalHits = hits.getTotalHits();
        long numHits = totalHits.value;   //检索命中数
        System.out.println("numHits：" + numHits);
        TotalHits.Relation relation = totalHits.relation;
        System.out.println("relation：" + relation);
        SearchHit[] searchHits = hits.getHits();
        for(SearchHit hit : searchHits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }

}
