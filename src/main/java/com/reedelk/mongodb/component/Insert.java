package com.reedelk.mongodb.component;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.reedelk.runtime.api.annotation.ModuleComponent;
import com.reedelk.runtime.api.annotation.Property;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import org.bson.Document;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ServiceScope;

import java.util.Map;

import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotBlank;
import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotNull;

@ModuleComponent("MongoDB Insert")
@Component(service = Insert.class, scope = ServiceScope.PROTOTYPE)
public class Insert implements ProcessorSync {

    @Property("Connection")
    private MongoDBConnection connection;

    @Property("Collection")
    private String collection;

    private MongoClient client;
    private String database;

    @Override
    public void initialize() {
        requireNotNull(Insert.class, connection, "MongoDB connection must not be null");

        String connectionURL = connection.getConnectionURL();
        requireNotBlank(Insert.class, connectionURL, "MongoDB connection url must not be empty");

        this.database = connection.getDatabase();
        requireNotBlank(Insert.class, database, "MongoDB database must not be null");

        requireNotBlank(Insert.class, collection, "MongoDB collection must not be empty");

        MongoClientSettings settings = MongoClientSettings.builder()
                // TODO: Add credentials
                .applyConnectionString(new ConnectionString(connectionURL))
                .build();

        client = MongoClients.create(settings);
    }

    @Override
    public Message apply(FlowContext flowContext, Message message) {
        MongoDatabase mongoDatabase = client.getDatabase(database);

        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);

        // TODO: NOte that the payload could be a map as well!

        Object payload = message.payload();

        if (payload instanceof String) {

            String payloadString = (String) payload;

            Document document = Document.parse(payloadString);

            mongoCollection.insertOne(document);

        } else if (payload instanceof Map) {
            // TODO: Add a check that they are all keys as string...
            Map<String, Object> payloadMap = (Map<String, Object>) payload;

            Document document = new Document(payloadMap);

            mongoCollection.insertOne(document);

        } else {
            throw new IllegalArgumentException("Not good type");
        }

        return message;
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

    public void setCollection(String collection) {
        this.collection = collection;
    }
}
