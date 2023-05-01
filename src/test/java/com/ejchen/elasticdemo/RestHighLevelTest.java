package com.ejchen.elasticdemo;

import com.ejchen.elasticdemo.entity.Product;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.List;

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
     * 简单创建索引
     * @throws IOException
     */
    @Test
    public void createIndex() throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("productlist");
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    /**
     * 查看索引的详细信息
     * @throws IOException
     */
    @Test
    public void getIndex() throws IOException {
        GetIndexRequest user_test = new GetIndexRequest("kibana_sample_data_flights");
        GetIndexResponse getIndexResponse = restHighLevelClient.indices().get(user_test, RequestOptions.DEFAULT);
        System.out.println(getIndexResponse.getAliases());
        System.out.println(getIndexResponse.getMappings());
        System.out.println(getIndexResponse.getSettings());
    }

    /**
     * 删除索引
     * @throws IOException
     */
    @Test
    public void deleteIndex() throws IOException {
        AcknowledgedResponse user_test = restHighLevelClient.indices().delete(new DeleteIndexRequest("productlist"), RequestOptions.DEFAULT);
        System.out.println(user_test.isAcknowledged());
    }

    /**
     * 在指定索引下，创建一条doc数据
     * @throws IOException
     */
    @Test
    public void insertDoc() throws IOException {
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index("productlist");
        indexRequest.id("1002");
        Product product = new Product();
        product.setId(2);
        product.setPrice(42.45);
        product.setTitle("键盘");
        product.setDescription("this is a 键盘！");

        ObjectMapper mapper = new ObjectMapper();
        String productJson = mapper.writeValueAsString(product);

        indexRequest.source(productJson,XContentType.JSON);
        IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(response.getResult());

    }

    /**
     * 局部数据更新
     * @throws IOException
     */
    @Test
    public void updateDoc() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("productlist").id("1001");
        updateRequest.doc(XContentType.JSON,"price",56.33);
        UpdateResponse response = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(response.getResult());
    }

    /**
     * 文档查询：根据_id查询doc
     * @throws IOException
     */
    @Test
    public void getDoc() throws IOException {
        GetRequest getRequest = new GetRequest();
        getRequest.index("productlist").id("1001");
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsString());
    }

    /**
     * 文档删除：根据_id删除
     * @throws IOException
     */
    @Test
    public void deleteDoc() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest();
        deleteRequest.index("productlist").id("1001");
        DeleteResponse response = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 批量添加
     * @throws IOException
     */
    @Test
    public void insertBath() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest source1 = new IndexRequest().index("productlist").id("1001").source(XContentType.JSON, "title", "鼠标","price",31,"description","鼠标的描述！","type",1);
        bulkRequest.add(source1);
        IndexRequest source2 = new IndexRequest().index("productlist").id("1002").source(XContentType.JSON, "title", "显示器","price",45,"description","显示器的描述！","type",1);
        bulkRequest.add(source2);
        IndexRequest source3 = new IndexRequest().index("productlist").id("1003").source(XContentType.JSON, "title", "主板","price",67,"description","主板的描述！","type",1);
        bulkRequest.add(source3);
        IndexRequest source4 = new IndexRequest().index("productlist").id("1004").source(XContentType.JSON, "title", "可乐","price",52,"description","可乐的描述！","type",2);
        bulkRequest.add(source4);
        IndexRequest source5 = new IndexRequest().index("productlist").id("1005").source(XContentType.JSON, "title", "炸鸡","price",62,"description","炸鸡的描述！","type",2);
        bulkRequest.add(source5);
        IndexRequest source6 = new IndexRequest().index("productlist").id("1006").source(XContentType.JSON, "title", "肯德基","price",34,"description","肯德基的描述！","type",3);
        bulkRequest.add(source6);
        BulkResponse responses = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(responses.getTook());
        System.out.println(responses.getItems());
    }


    /**
     * 查询所有
     * @throws IOException
     */
    @Test
    public void docQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("productlist");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits searchHits = response.getHits();
        System.out.println(searchHits.getTotalHits());
        System.out.println(response.getTook());

        for (SearchHit searchHit : searchHits) {
            System.out.println(searchHit.getSourceAsString());
        }
    }

    /**
     * 条件查询
     * @throws IOException
     */
    @Test
    public void docTermQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("productlist");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("price",52));
        searchRequest.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits searchHits = response.getHits();
        System.out.println(searchHits.getTotalHits());
        System.out.println(response.getTook());

        for (SearchHit searchHit : searchHits) {
            System.out.println(searchHit.getSourceAsString());
        }
    }

    /**
     * 分页查询
     * @throws IOException
     */
    @Test
    public void docPageQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("productlist");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.from(6);
        sourceBuilder.size(3);
        searchRequest.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits searchHits = response.getHits();
        System.out.println(searchHits.getTotalHits());
        System.out.println(response.getTook());

        for (SearchHit searchHit : searchHits) {
            System.out.println(searchHit.getSourceAsString());
        }
    }

    /**
     * 条件查询-结果排序
     * @throws IOException
     */
    @Test
    public void docSortQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("productlist");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.sort("id");
        searchRequest.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits searchHits = response.getHits();
        System.out.println(searchHits.getTotalHits());
        System.out.println(response.getTook());

        for (SearchHit searchHit : searchHits) {
            System.out.println(searchHit.getSourceAsString());
        }
    }

    /**
     * 条件查询-过滤字段
     * @throws IOException
     */
    @Test
    public void docFilterQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("productlist");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
//        sourceBuilder.sort("id");
        String[] excludes = {"id"};
        String[] includes = {"title","price"};
        sourceBuilder.fetchSource(includes,excludes);

        searchRequest.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits searchHits = response.getHits();
        System.out.println(searchHits.getTotalHits());
        System.out.println(response.getTook());

        for (SearchHit searchHit : searchHits) {
            System.out.println(searchHit.getSourceAsString());
        }

    }


    /**
     * 批量删除
     * @throws IOException
     */
    @Test
    public void deleteBatch() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        DeleteRequest request = new DeleteRequest().index("productlist").id("1004");
        bulkRequest.add(request);
        DeleteRequest request2 = new DeleteRequest().index("productlist").id("1001");
        bulkRequest.add(request2);
        BulkResponse responses = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(responses.getTook());
        System.out.println(responses.getItems());
    }

    /**
     * 条件查询-组合查询
     * @throws IOException
     */
    @Test
    public void docGroupQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("productlist");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("price",521));
        boolQueryBuilder.must(QueryBuilders.matchQuery("title","可乐"));
        sourceBuilder.query(boolQueryBuilder);
        searchRequest.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits searchHits = response.getHits();
        System.out.println(searchHits.getTotalHits());
        System.out.println(response.getTook());

        for (SearchHit searchHit : searchHits) {
            System.out.println(searchHit.getSourceAsString());
        }
    }

    /**
     * 条件查询-分组查询
     * @throws IOException
     */
    @Test
    public void docGroupByQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest("productlist");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("type_group").field("type");
        sourceBuilder
                .query(QueryBuilders.matchAllQuery())
                .aggregation(aggregationBuilder)
                .size(0);

        searchRequest.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        Aggregations aggregations = response.getAggregations();
        ParsedLongTerms terms = aggregations.get("type_group");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            System.out.println(bucket.getKey() + "," + bucket.getDocCount());
        }
    }

    /**
     * 聚合查询-max(ParsedMax) min(ParsedMin) sum(ParsedSum) avg(ParseAvg)
     * @throws IOException
     */
    @Test
    public void docSumAggQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest("productlist");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum("sum_price").field("price");
        sourceBuilder
                .query(QueryBuilders.matchAllQuery())
                .aggregation(sumAggregationBuilder)
                .size(0);

        searchRequest.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        Aggregations aggregations = response.getAggregations();
        ParsedSum sum = aggregations.get("sum_price");
        System.out.println(sum.getValue());

    }

    @Test
    public void docAvgAggQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest("productlist");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        AvgAggregationBuilder avgAggregationBuilder = AggregationBuilders.avg("avg_price").field("price");
        sourceBuilder
                .query(QueryBuilders.matchAllQuery())
                .aggregation(avgAggregationBuilder)
                .size(0);

        searchRequest.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        Aggregations aggregations = response.getAggregations();
        ParsedAvg avg = aggregations.get("avg_price");
        System.out.println(avg.getValue());

    }

    @Test
    public void docMaxAggQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest("productlist");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        MaxAggregationBuilder maxAggregationBuilder = AggregationBuilders.max("max_price").field("price");
        sourceBuilder
                .query(QueryBuilders.matchAllQuery())
                .aggregation(maxAggregationBuilder)
                .size(0);

        searchRequest.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        Aggregations aggregations = response.getAggregations();
        ParsedMax max_price = aggregations.get("max_price");
        System.out.println(max_price.getValue());

    }

    @Test
    public void docMinAggQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest("productlist");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        MinAggregationBuilder minAggregationBuilder = AggregationBuilders.min("min_price").field("price");
        sourceBuilder
                .query(QueryBuilders.matchAllQuery())
                .aggregation(minAggregationBuilder)
                .size(0);

        searchRequest.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        Aggregations aggregations = response.getAggregations();
        ParsedMin min = aggregations.get("min_price");
        System.out.println(min.getValue());

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
