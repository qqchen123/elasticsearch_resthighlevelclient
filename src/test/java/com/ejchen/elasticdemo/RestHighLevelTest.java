package com.ejchen.elasticdemo;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

/**
 * @PROJECT_NAME: elastic-demo
 * @DESCRIPTION:
 * @USER: ejchen
 * @DATE: 2023/3/5 14:16
 */
public class RestHighLevelTest extends ElasticDemoApplicationTests {
    private final RestHighLevelClient restHighLevelClient;

    @Autowired
    public RestHighLevelTest(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * 创建索引 创建对象
     */
    @Test
    public void testIndexAndMapping() throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("products");
        createIndexRequest.mapping("_doc", "{\n" +
                "    \"properties\": {\n" +
                "      \"id\":{\n" +
                "        \"type\": \"integer\"\n" +
                "      },\n" +
                "      \"title\":{\n" +
                "        \"type\": \"keyword\"\n" +
                "      },\n" +
                "      \"price\":{\n" +
                "        \"type\": \"double\"\n" +
                "      },\n" +
                "      \"created_at\":{\n" +
                "        \"type\": \"date\"\n" +
                "      },\n" +
                "      \"description\":{\n" +
                "        \"type\": \"text\",\n" +
                "        \"analyzer\": \"ik_max_word\"\n" +
                "      }\n" +
                "    }\n" +
                "  }", XContentType.JSON);
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    /**
     * 删除索引
     *
     * @throws IOException
     */
    @Test
    public void testDeleteIndex() throws IOException {

        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("products");
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());

    }

    /**
     * 索引一条文档(创建)
     *
     * @throws IOException
     */
    @Test
    public void testCreateDoc() throws IOException {
        IndexRequest indexRequest = new IndexRequest("products");
        indexRequest
                .id("2")
                .source("{\n" +
                        "  \"id\":2,\n" +
                        "  \"title\":\"vivo12\",\n" +
                        "  \"price\":2999.99,\n" +
                        "  \"created_at\":\"2021-09-15\",\n" +
                        "  \"description\":\"vivo12 是一个非常流畅的手机，哈哈哈！\"\n" +
                        "}", XContentType.JSON);
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse.status());
    }


    /**
     * 更新一条文档(创建)
     *
     * @throws IOException
     */
    @Test
    public void testUpdateDoc() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("products", "1");
        updateRequest
                .doc("{\n" +
                        "  \"price\":5999.99\n" +
                        "}", XContentType.JSON);
        UpdateResponse indexResponse = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse.status());
    }

    /**
     * 删除一条文档(创建)
     *
     * @throws IOException
     */
    @Test
    public void testDeleteDoc() throws IOException {

        DeleteResponse delete = restHighLevelClient.delete(new DeleteRequest("products", "2"), RequestOptions.DEFAULT);
        System.out.println(delete);
    }

    /**
     * 基于id查询一条文档
     *
     * @throws IOException
     */
    @Test
    public void testQueryByIdDoc() throws IOException {

        GetRequest products = new GetRequest("products", "1");
        GetResponse getResponse = restHighLevelClient.get(products, RequestOptions.DEFAULT);
        System.out.println(getResponse.getId());
        System.out.println(getResponse.getSource());
    }


    /**
     * 查询所有
     *
     * @throws IOException
     */
    @Test
    public void testMatchAll() throws IOException {
        //        指定搜索索引
        SearchRequest searchRequest = new SearchRequest("products");
//        指定条件对象
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());//查询所有
//        指定查询条件
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println(searchResponse.getHits().getTotalHits().value);
        System.out.println(searchResponse.getHits().getMaxScore());
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getId());
            System.out.println(hit.getSourceAsString());
        }
    }
    /**
     * 不同条件查询 term（关键词查询）
     *
     * @throws IOException
     */
    @Test
    public void testQuery() throws IOException {
//        query(QueryBuilders.termQuery("description","vivo"));
//        query(QueryBuilders.rangeQuery("price").gte(1000).lte(10000));
//        query(QueryBuilders.prefixQuery("description","iphon"));
//        query(QueryBuilders.wildcardQuery("title","i*"));
//        query(QueryBuilders.idsQuery().addIds("1").addIds("2"));
//        query(QueryBuilders.multiMatchQuery("非常").field("description"));
        query(QueryBuilders.multiMatchQuery("12","description","title"));
    }


    public void query(QueryBuilder queryBuilder) throws IOException {
//        指定搜索索引
        SearchRequest searchRequest = new SearchRequest("products");
        //        指定条件对象
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);//查询条件
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println(searchResponse.getHits().getTotalHits().value);
        System.out.println(searchResponse.getHits().getMaxScore());
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getId());
            System.out.println(hit.getSourceAsString());
        }
    }




}
