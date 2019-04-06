/*
 * |-------------------------------------------------
 * | Copyright © 2019 Colin But. All rights reserved.
 * |-------------------------------------------------
 */
package com.mycompany.aws.dynamodb.java;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public final class Crud {

    private Crud(){}

    public static void createNewItem() {
        DynamoDB dynamoDB = AmazonDynamoDBClientManager.getDynamoDB();
        Table table = dynamoDB.getTable("Movies");

        int year = 2015;
        String title = "The Big New Movie";

        final Map<String, Object> infoMap = new HashMap<String, Object>();
        infoMap.put("plot", "Nothing happens at all.");
        infoMap.put("rating", 0);

        Item item = new Item()
            .withPrimaryKey("year", year, "title", title)
            .withMap("info", infoMap);

        try {
            PutItemOutcome putItemOutcome = table.putItem(item);
            System.out.println("PutItem succeeded:\n" + putItemOutcome.getPutItemResult());
        } catch (Exception ex) {
            System.err.println("Unable to add movie: " + year + " " + title);
            System.err.println(ex.getMessage());
        }
    }

    public static void readItem() {
        DynamoDB dynamoDB = AmazonDynamoDBClientManager.getDynamoDB();
        Table table = dynamoDB.getTable("Movies");

        int year = 2015;
        String title = "The Big New Movie";

        GetItemSpec spec = new GetItemSpec()
            .withPrimaryKey("year", year, "title", title);

        try {
            Item item = table.getItem(spec);
        } catch (Exception ex) {
            System.err.println("Unable to read item: " + year + " " + title);
            System.err.println(ex.getMessage());
        }
    }

    public static void updateItem() {
        DynamoDB dynamoDB = AmazonDynamoDBClientManager.getDynamoDB();
        Table table = dynamoDB.getTable("Movies");

        int year = 2015;
        String title = "The Big New Movie";

        ValueMap valueMap = new ValueMap()
            .withNumber(":r", 5.5)
            .withString(":p", "Everything happens all at once")
            .withList(":a", Arrays.asList("Harry", "James", "Danny"));

        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
            .withPrimaryKey("year", year, "title", title)
            .withUpdateExpression("set info.rating = :r, info.plot = :p, info.actors = :a")
            .withValueMap(valueMap)
            .withReturnValues(ReturnValue.UPDATED_NEW);

        try {
            System.out.println("Updating item: " + updateItemSpec);
            UpdateItemOutcome updateItemOutcome = table.updateItem(updateItemSpec);
            System.out.println("UpdateItem succeeded:\n" + updateItemOutcome.getItem().toJSONPretty());
        } catch (Exception ex) {
            System.err.println("Unable to update movie: " + year + " " + title);
            System.err.println(ex.getMessage());
        }
    }

    public static void incrementAtomicCounters() {
        DynamoDB dynamoDB = AmazonDynamoDBClientManager.getDynamoDB();
        Table table = dynamoDB.getTable("Movies");

        int year = 2015;
        String title = "The Big New Movie";

        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
            .withPrimaryKey("year", year, "title", title)
            .withUpdateExpression("set info.rating = info.rating + :val")
            .withValueMap(new ValueMap().withNumber(":val", 1))
            .withReturnValues(ReturnValue.UPDATED_NEW);

        try {
            System.out.println("Incrementing an atomic counter...");
            UpdateItemOutcome updateItemOutcome = table.updateItem(updateItemSpec);
            System.out.println("UpdateItem succeeded:\n" + updateItemOutcome.getItem().toJSONPretty());
        } catch (Exception ex) {
            System.err.println("Unable to update movie: " + year + " " + title);
            System.err.println(ex.getMessage());
        }
    }

    public static void conditionalUpdateItem() {
        DynamoDB dynamoDB = AmazonDynamoDBClientManager.getDynamoDB();
        Table table = dynamoDB.getTable("Movies");

        int year = 2015;
        String title = "The Big New Movie";

        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
            .withPrimaryKey(new PrimaryKey("year", year, "title", title))
            .withUpdateExpression("remove info.actors[0")
            .withConditionExpression("size(info.actors) > :num")
            .withValueMap(new ValueMap().withNumber(":val", 3))
            .withReturnValues(ReturnValue.UPDATED_NEW);

        try {
            System.out.println("Going for a conditional update");
            UpdateItemOutcome updateItemOutcome = table.updateItem(updateItemSpec);
            System.out.println("UpdateItem succeeded:\n" + updateItemOutcome.getItem().toJSONPretty());
        } catch (Exception ex) {
            System.err.println("Unable to update movie: " + year + " " + title);
            System.err.println(ex.getMessage());
        }
    }

    public static void deleteItem() {
        DynamoDB dynamoDB = AmazonDynamoDBClientManager.getDynamoDB();
        Table table = dynamoDB.getTable("Movies");

        int year = 2015;
        String title = "The Big New Movie";

        DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
            .withPrimaryKey(new PrimaryKey("year", year, "title", title))
            .withConditionExpression("info.rating <= :val")
            .withValueMap(new ValueMap().withNumber(":val", 5.0));

        try {
            System.out.println("Attempting to conditionally delete an item...");
            table.deleteItem(deleteItemSpec);
            System.out.println("Deleted item successfully");
        } catch (Exception ex) {
            System.err.println("Unable to delete movie: " + year + " " + title);
            System.err.println(ex.getMessage());
        }
    }
}
