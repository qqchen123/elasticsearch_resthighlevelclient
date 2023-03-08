package com.ejchen.elasticdemo.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

/**
 * @PROJECT_NAME: elastic-demo
 * @DESCRIPTION:
 * @USER: ejchen
 * @DATE: 2023/3/5 12:29
 */
@Data
@Document(indexName = "products",createIndex = true)
public class Product {
    @Id
    private Integer id;
    @Field(type = FieldType.Keyword)
    private String title;
    @Field(type = FieldType.Double)
    private Double price;
    @Field(type = FieldType.Text)
    private String description;
}
