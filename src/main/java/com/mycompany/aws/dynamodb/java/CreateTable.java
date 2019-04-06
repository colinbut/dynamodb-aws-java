/*
 * |-------------------------------------------------
 * | Copyright © 2019 Colin But. All rights reserved.
 * |-------------------------------------------------
 */
package com.mycompany.aws.dynamodb.java;

import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import java.util.Arrays;
import java.util.List;

public final class CreateTable {

    private CreateTable(){}

    public static void createTable(String tableName) {
        String hashKey = "year";
        String rangeKey = "title";

        try {
            System.out.println("Attempting to create table; please wait...");

            List<KeySchemaElement> keySchemaElements = Arrays.asList(new KeySchemaElement(hashKey, KeyType.HASH),
                new KeySchemaElement(rangeKey, KeyType.RANGE));

            List<AttributeDefinition> attributeDefinitions = Arrays.asList(new AttributeDefinition(hashKey, ScalarAttributeType.N),
                new AttributeDefinition(rangeKey, ScalarAttributeType.S));

            Table table = AmazonDynamoDBClientManager.getDynamoDB().createTable(tableName, keySchemaElements,attributeDefinitions,
                new ProvisionedThroughput(10L, 10L));

            table.waitForActive();

            System.out.println(String.format("Success... Created table %s, table status: %s", table.getTableName(), table.getDescription()));

        } catch (Exception ex) {
            System.err.println("Unable to create table: ");
            System.err.println(ex.getMessage());
        }
    }
}
