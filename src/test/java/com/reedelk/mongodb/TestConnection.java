package com.reedelk.mongodb;

import com.mongodb.client.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;

public class TestConnection {


    @Test
    void shouldConnect() throws InterruptedException {
        MongoClient mongoClient = MongoClients.create("mongodb://172.17.0.2:27017");



        MongoDatabase hello = mongoClient.getDatabase("Hello");

        MongoCollection<Document> myCollection = hello.getCollection("MyCollection");
        myCollection.insertOne(Document.parse("{'name':'Mark'}"));

        Thread.sleep(10000);
        mongoClient.close();

    }
}
