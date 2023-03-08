package com.ejchen.elasticdemo;

import com.ejchen.elasticdemo.entity.Product;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.ByQueryResponse;
import org.springframework.data.elasticsearch.core.query.Query;

/**
 * @PROJECT_NAME: elastic-demo
 * @DESCRIPTION:
 * @USER: ejchen
 * @DATE: 2023/3/5 12:35
 */

public class ElasticSearchOptionsTest extends ElasticDemoApplicationTests{
    private final ElasticsearchOperations elasticsearchOperations;

    @Autowired
    public ElasticSearchOptionsTest(ElasticsearchOperations elasticsearchOperations){
        this.elasticsearchOperations = elasticsearchOperations;
    }

    /**
     * 索引一条文档,更新一条文档；
     * 当文档id不存在添加文档，否则更新文档
     */

    @Test
    public void  testIndex(){
        Product product = new Product();
        product.setId(2);
        product.setTitle("日本豆");
        product.setPrice(4.5);
        product.setDescription("日本豆真好吃，曾经非常爱吃！");
        elasticsearchOperations.save(product);

    }

    /**
     * 查询
     */
    @Test
    public void testSearch(){
        Product product = elasticsearchOperations.get("1", Product.class);
        System.out.println(product);
    }

    @Test
    public void testDelete(){
        Product product = new Product();
        product.setId(1);
        String delete = elasticsearchOperations.delete(product);
        System.out.println(delete);
    }

    @Test
    public void testDeleteAll(){
        elasticsearchOperations.delete(Query.findAll(), Product.class);
    }

    @Test
    public void testSearchAll(){
        SearchHits<Product> searchHits = elasticsearchOperations.search(Query.findAll(), Product.class);
        System.out.println(searchHits);
    }

}
