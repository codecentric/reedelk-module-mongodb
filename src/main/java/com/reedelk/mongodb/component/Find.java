package com.reedelk.mongodb.component;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.reedelk.runtime.api.annotation.DefaultValue;
import com.reedelk.runtime.api.annotation.InitValue;
import com.reedelk.runtime.api.annotation.ModuleComponent;
import com.reedelk.runtime.api.annotation.Property;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicvalue.DynamicString;
import org.bson.Document;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ServiceScope;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotBlank;
import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotNull;

@ModuleComponent("MongoDB Find")
@Component(service = Find.class, scope = ServiceScope.PROTOTYPE)
public class Find implements ProcessorSync {

    @Property("Connection")
    private MongoDBConnection connection;

    @Property("Collection")
    private String collection;

    @Property("Find Filter")
    @InitValue("#[message.payload()]")
    @DefaultValue("#[message.payload()")
    private DynamicString findFilter;

    @Reference
    private ScriptEngineService scriptService;

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

        return scriptService.evaluate(findFilter, flowContext, message).map(evaluatedFindExpression -> {

            Document filter = Document.parse(evaluatedFindExpression);
            FindIterable<Document> documents = mongoCollection.find(filter);

            List<String> output = new ArrayList<>();
            documents.forEach((Consumer<Document>) document -> output.add(document.toJson()));

            return MessageBuilder.get()
                    .withList(output, String.class)
                    .build();

        }).orElseGet(() -> {

            FindIterable<Document> documents = mongoCollection.find();

            List<String> output = new ArrayList<>();
            documents.forEach((Consumer<Document>) document -> output.add(document.toJson()));

            return MessageBuilder.get()
                    .withList(output, String.class)
                    .build();
        });
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

    public void setFindFilter(DynamicString findFilter) {
        this.findFilter = findFilter;
    }
}
