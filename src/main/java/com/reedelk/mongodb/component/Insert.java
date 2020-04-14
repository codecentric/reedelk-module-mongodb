package com.reedelk.mongodb.component;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import org.bson.Document;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ServiceScope;

@ModuleComponent("MongoDB Insert")
@Component(service = Insert.class, scope = ServiceScope.PROTOTYPE)
public class Insert implements ProcessorSync {

    @Property("Connection")
    private MongoDBConnection connection;

    @Property("Connection String")
    @InitValue("mongodb://localhost:27017")
    @Example("mongodb://localhost:27017")
    @Description("Connection string to Mongo DB")
    private String connectionString;

    @Property("Collection")
    private String collection;

    private MongoClient client;

    @Override
    public void initialize() {
        client = MongoClients.create(connectionString);
    }

    @Override
    public Message apply(FlowContext flowContext, Message message) {
        MongoDatabase database = client.getDatabase("mydb");

        MongoCollection<Document> collection = database.getCollection("test");

        String payload = message.payload(); // TODO: NOte that the payload could be a map as well!

        Document document = Document.parse(payload);

        collection.insertOne(document);

        return message;
    }

    @Override
    public void dispose() {
        if (client != null) {
            client.close();
        }
    }

    public MongoDBConnection getConnection() {
        return connection;
    }

    public void setConnection(MongoDBConnection connection) {
        this.connection = connection;
    }
}
