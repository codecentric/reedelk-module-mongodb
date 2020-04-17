package com.reedelk.mongodb.component;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.mongodb.internal.commons.DocumentUtils;
import com.reedelk.mongodb.internal.commons.Utils;
import com.reedelk.mongodb.internal.exception.MongoDBDeleteException;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.commons.ImmutableMap;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicvalue.DynamicObject;
import org.bson.Document;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ServiceScope;

import java.io.Serializable;
import java.util.Map;

import static com.reedelk.mongodb.internal.commons.Messages.Delete.DELETE_FILTER_NULL;
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
    ScriptEngineService scriptService;
    @Reference
    ClientFactory clientFactory;

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

        Object evaluatedFilter = scriptService.evaluate(filter, flowContext, message)
                .orElseThrow(() -> new MongoDBDeleteException(DELETE_FILTER_NULL.format(filter.value())));

        Document deleteFilter = DocumentUtils.from(evaluatedFilter);

        DeleteResult deleteResult = Utils.isTrue(many) ?
                mongoCollection.deleteMany(deleteFilter) :
                mongoCollection.deleteOne(deleteFilter);

        long deletedCount = deleteResult.getDeletedCount();
        boolean acknowledged = deleteResult.wasAcknowledged();

        Map<String, Serializable> componentAttributes = ImmutableMap.of(
                "deleteCount", deletedCount,
                "acknowledge", acknowledged);

        // TODO: Check return type;
        return MessageBuilder.get(Delete.class)
                .attributes(componentAttributes)
                .empty()
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

    public void setMany(Boolean many) {
        this.many = many;
    }
}
