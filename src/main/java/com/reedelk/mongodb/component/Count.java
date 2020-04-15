package com.reedelk.mongodb.component;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.mongodb.internal.commons.DocumentUtils;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.commons.DynamicValueUtils;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.exception.PlatformException;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicvalue.DynamicObject;
import org.bson.Document;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ServiceScope;

import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotBlank;

@ModuleComponent("MongoDB Count")
@Component(service = Count.class, scope = ServiceScope.PROTOTYPE)
public class Count implements ProcessorSync {

    @Property("Connection")
    @Description("MongoDB connection configuration to be used by this count operation. " +
            "Shared configurations use the same MongoDB client.")
    private ConnectionConfiguration connection;

    @Property("Collection")
    @Hint("MyCollection")
    @Example("MyCollection")
    @Description("Sets the name of the MongoDB collection to be used for the count operation.")
    private String collection;

    @Property("Filter")
    private DynamicObject filter;

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

        long count;

        if (DynamicValueUtils.isNotNullOrBlank(filter)) {
            Object filterObject = scriptService.evaluate(filter, flowContext, message)
                    .orElseThrow(() -> new PlatformException("Error could not evaluate filter."));

            Document filterDocument = DocumentUtils.from(filterObject);
            count = mongoCollection.countDocuments(filterDocument);

        } else {
            count = mongoCollection.countDocuments();
        }

        return MessageBuilder.get()
                .withJavaObject(count)
                .build();
    }

    @Override
    public void dispose() {
        clientFactory.dispose(this, connection);
        client = null;
    }

    public void setConnection(ConnectionConfiguration connection) {
        this.connection = connection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public void setFilter(DynamicObject filter) {
        this.filter = filter;
    }
}
