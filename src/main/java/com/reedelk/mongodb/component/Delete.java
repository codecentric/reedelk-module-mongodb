package com.reedelk.mongodb.component;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.mongodb.internal.commons.DocumentUtils;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.commons.ImmutableMap;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.exception.PlatformException;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.DefaultMessageAttributes;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageAttributes;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicvalue.DynamicObject;
import org.bson.Document;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ServiceScope;

import java.io.Serializable;
import java.util.Map;

import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotBlank;

@ModuleComponent("MongoDB Delete (One/Many)")
@Component(service = Delete.class, scope = ServiceScope.PROTOTYPE)
public class Delete implements ProcessorSync {

    @Property("Connection")
    @Description("MongoDB connection configuration to be used by this delete operation. " +
            "Shared configurations use the same MongoDB client.")
    private ConnectionConfiguration connection;

    @Property("Collection")
    @Hint("MyCollection")
    @Example("MyCollection")
    @Description("Sets the name of the MongoDB collection to be used for the delete operation.")
    private String collection;

    @Property("Delete Filter")
    @InitValue("#[message.payload()]")
    @DefaultValue("#[message.payload()")
    private DynamicObject filter;

    @Property("Delete Many")
    private Boolean many;

    @Reference
    private ScriptEngineService scriptService;
    @Reference
    private ClientFactory clientFactory;

    private MongoClient client;

    @Override
    public void initialize() {
        requireNotBlank(Delete.class, collection, "MongoDB collection must not be empty");

        this.client = clientFactory.clientByConfig(this, connection);
    }

    @Override
    public Message apply(FlowContext flowContext, Message message) {

        MongoDatabase mongoDatabase = client.getDatabase(connection.getDatabase());

        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);

        return scriptService.evaluate(filter, flowContext, message).map(evaluatedDeleteExpression -> {

            Document deleteFilter = DocumentUtils.from(evaluatedDeleteExpression);

            DeleteResult deleteResult;
            if (many != null && many) {
                deleteResult = mongoCollection.deleteMany(deleteFilter);
            } else {
                deleteResult = mongoCollection.deleteOne(deleteFilter);
            }

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
        clientFactory.dispose(this, connection);
        client = null;
    }

    public ConnectionConfiguration getConnection() {
        return connection;
    }

    public void setConnection(ConnectionConfiguration connection) {
        this.connection = connection;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public DynamicObject getFilter() {
        return filter;
    }

    public void setFilter(DynamicObject filter) {
        this.filter = filter;
    }

    public void setMany(Boolean many) {
        this.many = many;
    }
}
