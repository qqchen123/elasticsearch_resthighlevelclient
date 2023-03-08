package com.ejchen.elasticdemo;

import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @PROJECT_NAME: elastic-demo
 * @DESCRIPTION:
 * @USER: ejchen
 * @DATE: 2023/3/5 14:16
 */
public class RestHighLevelTest extends ElasticDemoApplicationTests{
    private final RestHighLevelClient restHighLevelClient;

    @Autowired
    public RestHighLevelTest(RestHighLevelClient restHighLevelClient){
        this.restHighLevelClient = restHighLevelClient;
    }

    @Test
    public void testIndexAndMapping(){

    }















}
