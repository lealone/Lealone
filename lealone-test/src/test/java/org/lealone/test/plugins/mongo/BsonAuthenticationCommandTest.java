/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.plugins.mongo;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.client.MongoClient;

public class BsonAuthenticationCommandTest extends MongoTestBase {

    @Before
    @Override
    public void before() {
    }

    @BeforeClass
    public static void beforeClass() {
    }

    @AfterClass
    public static void afterClass() {
    }

    // @Test
    public void testAuthentication1() {
        String authMechanism = "SCRAM-SHA-1";
        MongoClient mongoClient = getMongoClient("myUserAdmin", "mongo", authMechanism);
        mongoClient.getDatabase("admin").getCollection(collectionName).countDocuments();
        mongoClient.close();
    }

    @Test
    public void testAuthentication256() {
        String command = "{\"createUser\": \"myUserAdmin\", \"pwd\": \"mongo\", "
                + "\"roles\": [{\"role\": \"userAdminAnyDatabase\", \"db\": \"admin\"},"
                + " {\"role\": \"readWriteAnyDatabase\", \"db\": \"admin\"}], \"$db\": \"admin\"}";
        MongoClient mongoClient = getMongoClient();
        mongoClient.getDatabase("admin").runCommand(org.bson.Document.parse(command));
        mongoClient.close();

        String authMechanism = "SCRAM-SHA-256";
        // authMechanism = "SCRAM-SHA-512"; //不支持
        mongoClient = getMongoClient("myUserAdmin", "mongo", authMechanism);
        mongoClient.getDatabase("admin").getCollection(collectionName).countDocuments();
        mongoClient.close();
    }
}
