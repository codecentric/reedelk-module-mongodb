package com.reedelk.mongodb.component;

import com.mongodb.client.*;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import org.bson.Document;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ServiceScope;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@ModuleComponent("MongoDB Find")
@Component(service = Find.class, scope = ServiceScope.PROTOTYPE)
public class Find implements ProcessorSync {

    @Property("Connection")
    private MongoDBConnection connection;

    @Property("Connection String")
    @InitValue("mongodb://localhost:27017")
    @Example("mongodb://localhost:27017")
    @Description("Connection string to Mongo DB")
    private String connectionString;

    @Property("Collection")
    private String collection;

    @Property("Database")
    private String database;

    private MongoClient client;

    @Override
    public void initialize() {
        client = MongoClients.create(connectionString);
    }

    @Override
    public Message apply(FlowContext flowContext, Message message) {

        MongoDatabase mongoDatabase = client.getDatabase(database);

        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);

        FindIterable<Document> documents = mongoCollection.find();

        List<String> output = new ArrayList<>();
        documents.forEach((Consumer<Document>) document -> output.add(document.toJson()));

        return MessageBuilder.get()
                .withList(output, String.class)
                .build();
    }

    @Override
    public void dispose() {
        if (client != null) {
            client.close();
        }
    }

    public void setConnection(MongoDBConnection connection) {
        this.connection = connection;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public void setDatabase(String database) {
        this.database = database;
    }
}
