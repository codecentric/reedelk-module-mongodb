package com.reedelk.mongodb.component;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.reedelk.runtime.api.annotation.DefaultValue;
import com.reedelk.runtime.api.annotation.InitValue;
import com.reedelk.runtime.api.annotation.ModuleComponent;
import com.reedelk.runtime.api.annotation.Property;
import com.reedelk.runtime.api.commons.ImmutableMap;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.exception.PlatformException;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.DefaultMessageAttributes;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageAttributes;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicvalue.DynamicString;
import org.bson.Document;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ServiceScope;

import java.io.Serializable;
import java.util.Map;

import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotBlank;
import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotNull;

@ModuleComponent("MongoDB Delete")
@Component(service = Delete.class, scope = ServiceScope.PROTOTYPE)
public class Delete implements ProcessorSync {

    @Property("Connection")
    private MongoDBConnection connection;

    @Property("Collection")
    private String collection;

    @Property("Delete Filter")
    @InitValue("#[message.payload()]")
    @DefaultValue("#[message.payload()")
    private DynamicString deleteFilter;

    @Property("Delete Many")
    private Boolean deleteMany;

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

        return scriptService.evaluate(deleteFilter, flowContext, message).map(evaluatedDeleteExpression -> {

            Document deleteFilter = Document.parse(evaluatedDeleteExpression);

            DeleteResult deleteResult = mongoCollection.deleteOne(deleteFilter);

            long deletedCount = deleteResult.getDeletedCount();
            boolean acknowledged = deleteResult.wasAcknowledged();

            Map<String, Serializable> componentAttributes = ImmutableMap.of(
                    "deleteCount", deletedCount,
                    "acknowledge", acknowledged);
            MessageAttributes attributes = new DefaultMessageAttributes(Delete.class, componentAttributes);

            return MessageBuilder.get()
                    .attributes(attributes)
                    .empty()
                    .build();

        }).orElseThrow(() -> new PlatformException("Could not delete"));
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

    public void setDeleteFilter(DynamicString deleteFilter) {
        this.deleteFilter = deleteFilter;
    }
}
