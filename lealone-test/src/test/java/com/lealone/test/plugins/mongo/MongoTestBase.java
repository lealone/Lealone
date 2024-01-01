/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.plugins.mongo;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.lealone.plugins.mongo.server.MongoServer;
import com.lealone.test.UnitTestBase;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoTestBase extends UnitTestBase {

    public final static int TEST_PORT = 9310;

    protected static MongoClient mongoClient;
    protected static MongoDatabase database;
    protected MongoCollection<Document> collection;
    protected String collectionName;

    protected MongoTestBase() {
        this.collectionName = getClass().getSimpleName();
    }

    public MongoTestBase(String collectionName) {
        this.collectionName = collectionName;
    }

    @BeforeClass
    public static void beforeClass() {
        mongoClient = getMongoClient();
        database = mongoClient.getDatabase("mongo");
        // System.out.println(database.runCommand(Document.parse("{\"buildInfo\": 1}")));
        // database.createCollection(collectionName);
    }

    @AfterClass
    public static void afterClass() {
        mongoClient.close();
    }

    @Before
    public void before() {
        collection = database.getCollection(collectionName);
    }

    @Override
    public void runTest() {
        beforeClass();
        before();
        try {
            test();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            afterClass();
        }
    }

    @Override
    protected void test() throws Exception {
        // do nothing
    }

    public static MongoClient getMongoClient() {
        return getMongoClient(null, null, null);
    }

    public static MongoClient getMongoClient(String user, String password, String authMechanism) {
        int port = MongoServer.DEFAULT_PORT;
        port = TEST_PORT;
        String authenticationStr = "";
        if (user != null) {
            authenticationStr = user + ":" + password + "@";
        }
        String connectionString = "mongodb://" + authenticationStr + "127.0.0.1:" + port
                + "/?serverSelectionTimeoutMS=200000";
        if (authMechanism != null)
            connectionString += "&authMechanism=" + authMechanism;
        return MongoClients.create(connectionString);
    }

    public static void printBson(Bson bson) {
        System.out.println(
                bson.toBsonDocument(bson.getClass(), MongoClientSettings.getDefaultCodecRegistry())
                        .toJson());
    }
}
