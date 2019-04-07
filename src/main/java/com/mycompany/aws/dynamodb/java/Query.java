/*
 * |-------------------------------------------------
 * | Copyright © 2019 Colin But. All rights reserved.
 * |-------------------------------------------------
 */
package com.mycompany.aws.dynamodb.java;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class Query {

    private static final Logger LOGGER = Logger.getLogger("Query");

    public static void query() {
        DynamoDB dynamoDB = AmazonDynamoDBClientManager.getDynamoDB();
        Table table = dynamoDB.getTable("Movies");

        Map<String, String> nameMap = new HashMap<>();
        nameMap.put("#yr", "year");

        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put(":yyyy", 1985);

        QuerySpec querySpec = new QuerySpec()
            .withKeyConditionExpression("#yr = :yyyy")
            .withNameMap(nameMap)
            .withValueMap(valueMap);

        try {
            LOGGER.info("Movies from 1985");
            ItemCollection<QueryOutcome> items = table.query(querySpec);

            for (Item item : items) {
                LOGGER.info(item.getNumber("year") + " : " + item.getString("title"));
            }

        } catch (Exception ex) {
            LOGGER.severe("Unable to query movie");
            LOGGER.severe(ex.getMessage());
        }
    }
}
