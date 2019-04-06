/*
 * |-------------------------------------------------
 * | Copyright © 2019 Colin But. All rights reserved.
 * |-------------------------------------------------
 */
package com.mycompany.aws.dynamodb.java;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;

public final class DeleteTable {

    private DeleteTable(){}

    public static void deleteTable() {
        DynamoDB dynamoDB = AmazonDynamoDBClientManager.getDynamoDB();
        Table table = dynamoDB.getTable("Movies");

        try {
            System.out.println("Trying to delete table, please wait...");
            table.delete();
            table.waitForDelete();
            System.out.println("Successfully deleted table");
        } catch (Exception ex) {
            System.err.println("Unable to delete table: " + table.getTableName());
            System.err.println(ex.getMessage());
        }
    }
}
