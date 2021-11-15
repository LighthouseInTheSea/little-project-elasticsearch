package com.ruyuan.little.project;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ruyuan.little.project.elasticsearch.LittleProjectElasticsearchApplication;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@SpringBootTest(classes = LittleProjectElasticsearchApplication.class)
@RunWith(SpringRunner.class)
@Slf4j
public class RestClientTest {

    /**
     * es的客户端
     */
    @Autowired
    private RestHighLevelClient restClient;

    /**
     * es测试用的index
     */
    private final static String TEST_INDEX="test_index";

    /**
     * 初始化 删除索引和创建索引
     */
    @Test
    public void init() throws IOException {
        //删除索引
        this.deleteIndex(TEST_INDEX);
        //创建索引
        this.createIndex(TEST_INDEX);
    }

    /**
     * 创建索引
     * @param testIndex 索引名称
     */
    private void createIndex(String testIndex) throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(testIndex);
        createIndexRequest.settings(Settings.builder()
                .put("index.number_of_shards",5)
                .put("index.number_of_replicas",1));

        createIndexRequest.mapping("{\n"
                + "            \"properties\":{\n"
                + "                \"name\":{\n"
                + "                    \"type\":\"text\"\n"
                + "                }\n"
                + "            }\n"
                + "        }", XContentType.JSON);

        CreateIndexResponse createIndexResponse = restClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        log.info("create index {} response:{}",testIndex,createIndexResponse.isAcknowledged());
    }

    /**
     * 删除索引
     * @param testIndex 索引名称
     */
    private void deleteIndex(String testIndex) throws IOException{
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(testIndex);
        try {
            AcknowledgedResponse response = restClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            log.info("delete index:{} response:{}",testIndex,response.isAcknowledged());
        }catch (ElasticsearchStatusException e){
            log.info("delete fail", e);
        }
    }

    /**
     * 测试新增数据
     */
    @Test
    public void testInsert() throws IOException {
        Data data = new Data("1","测试数据01");
        this.insertData(data);
    }

    /**
     * 插入数据
     * @param data 数据
     */
    private void insertData(Data data) throws IOException{
        IndexRequest indexRequest = new IndexRequest(TEST_INDEX);
        indexRequest.id(data.getId());
        //强制刷新数据
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        indexRequest.source(JSONObject.toJSONString(data),XContentType.JSON);
        IndexResponse indexResponse = restClient.index(indexRequest, RequestOptions.DEFAULT);
        Assert.assertEquals(indexResponse.status(), RestStatus.CREATED);
    }

    /**
     * 测试查询操作
     * @throws IOException
     */
    @Test
    public void testQuery() throws IOException {
        Data data = new Data("2","测试数据02");
        //插入数据
        this.insertData(data);
        //查询数据
        this.queryById(data);
    }

    /**
     * 测试查询操作
     * @param data 查询数据
     */
    private void queryById(Data data) throws IOException{
        SearchRequest searchRequest = new SearchRequest(TEST_INDEX);
        searchRequest.source(new SearchSourceBuilder()
                .query(QueryBuilders.termQuery("id",data.getId())));

        SearchResponse searchResponse = restClient.search(searchRequest, RequestOptions.DEFAULT);
        Assert.assertEquals(searchResponse.status(),RestStatus.OK);

        //查询条数为1
        SearchHits hits = searchResponse.getHits();
        Assert.assertEquals(1,hits.getTotalHits().value);

        //判断查询数据和插入数据是否相等
        String dataJson = hits.getHits()[0].getSourceAsString();
        Assert.assertEquals(JSON.toJSONString(data),dataJson);
    }

    /**
     * 测试修改操作
     */
    @Test
    public void testUpdate() throws IOException {
        //插入数据
        Data data = new Data("3","测试数据03");
        this.insertData(data);

        //更新数据
        data.setName("测试数据被更新");
        UpdateRequest updateRequest = new UpdateRequest(TEST_INDEX, data.getId());
        updateRequest.doc(JSONObject.toJSONString(data),XContentType.JSON);
        //强制刷新数据
        updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        UpdateResponse updateResponse = restClient.update(updateRequest, RequestOptions.DEFAULT);
        Assert.assertEquals(updateResponse.status(),RestStatus.OK);

        //查询更新结果
        this.queryById(data);
    }


    /**
     * 测试删除操作
     */
    @Test
    public void testDelete() throws IOException {
        Data data = new Data("4","测试数据04");
        this.insertData(data);

        //删除数据
        DeleteRequest deleteRequest = new DeleteRequest(TEST_INDEX, data.getId());
        //强制刷新
        deleteRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        DeleteResponse deleteResponse = restClient.delete(deleteRequest, RequestOptions.DEFAULT);
        Assert.assertEquals(deleteResponse.status(),RestStatus.OK);

        //查询数据
        SearchRequest searchRequest = new SearchRequest(TEST_INDEX);
        searchRequest.source(new SearchSourceBuilder()
                .query(QueryBuilders.termQuery("id",data.getId())));

        SearchResponse searchResponse = restClient.search(searchRequest, RequestOptions.DEFAULT);
        Assert.assertEquals(searchResponse.status(),RestStatus.OK);

        //查询条数为1
        SearchHits hits = searchResponse.getHits();
        Assert.assertEquals(0,hits.getTotalHits().value);
    }

    /**
     * 测试实体
     */
    @lombok.Data
    @AllArgsConstructor
    static class Data {
        /**
         * 主键id
         */
        private String id;

        /**
         * 名称
         */
        private String name;
    }
}
