package com.reedelk.mongodb.component;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.mongodb.internal.commons.Attributes;
import com.reedelk.mongodb.internal.commons.DocumentUtils;
import com.reedelk.mongodb.internal.commons.Utils;
import com.reedelk.mongodb.internal.exception.MongoDBUpdateException;
import com.reedelk.runtime.api.annotation.*;
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

import static com.reedelk.mongodb.internal.commons.Messages.Update.UPDATE_DOCUMENT_EMPTY;
import static com.reedelk.mongodb.internal.commons.Messages.Update.UPDATE_FILTER_NULL;
import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotBlank;

@ModuleComponent("MongoDB Update (One/Many)")
@Component(service = Update.class, scope = ServiceScope.PROTOTYPE)
@Description("Updates one or more document into a MongoDB database on the specified collection. " +
        "The MongoDB connection configuration allows to specify host, port, database, username and password to be used for the MongoDB connection. " +
        "If the filter expression is not empty, the filter will be used to match only the document/s to be updated with the updated document. " +
        "The updated document can be a static or a dynamic expression. By default the message payload " +
        "is used as a update document. The update document could be a JSON string, " +
        "Map, Pair or DataRow (Update One). If the property many is true, <b>all</b> the documents matching the " +
        "given filter will be updated (Update Many).")
public class Update implements ProcessorSync {

    @Property("Connection")
    @Description("MongoDB connection configuration to be used by this update operation. " +
            "Shared configurations use the same MongoDB client.")
    private ConnectionConfiguration connection;

    @Property("Collection")
    @Hint("MyCollection")
    @Example("MyCollection")
    @Description("Sets the name of the MongoDB collection to be used for the update operation.")
    private String collection;

    @Property("Find Filter")
    @InitValue("#[message.payload()]")
    @DefaultValue("#[message.payload()")
    private DynamicObject filter;

    @Property("Updated Document")
    @InitValue("#[message.payload()]")
    @DefaultValue("#[message.payload()")
    private DynamicObject document;

    @Property("Update Many")
    @Group("Advanced")
    @Description("If true updates many documents ")
    private Boolean many;

    @Reference
    ScriptEngineService scriptService;
    @Reference
    ClientFactory clientFactory;

    private MongoClient client;

    @Override
    public void initialize() {
        requireNotBlank(Update.class, collection, "MongoDB collection must not be empty");
        this.client = clientFactory.clientByConfig(this, connection);
    }

    @Override
    public Message apply(FlowContext flowContext, Message message) {

        MongoDatabase mongoDatabase = client.getDatabase(connection.getDatabase());
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);

        Object evaluatedFilter = scriptService.evaluate(filter, flowContext, message)
                .orElseThrow(() -> new MongoDBUpdateException(UPDATE_FILTER_NULL.format(filter.value())));

        Object toUpdate = scriptService.evaluate(document, flowContext, message)
                .orElseThrow(() -> new MongoDBUpdateException(UPDATE_DOCUMENT_EMPTY.format(document.value())));

        UpdateResult updateResult;

        // Update without pipeline
        Document toUpdateFilter = DocumentUtils.from(evaluatedFilter);
        Document toUpdateDocument = DocumentUtils.from(toUpdate);

        String json = toUpdateDocument.toJson();
        updateResult = Utils.isTrue(many) ?
                mongoCollection.updateMany(toUpdateFilter, toUpdateDocument) :
                mongoCollection.updateOne(toUpdateFilter, toUpdateDocument);

        long modifiedCount = updateResult.getModifiedCount();

        Map<String, Serializable> componentAttributes = Attributes.from(updateResult);
        return MessageBuilder.get(Update.class)
                .attributes(componentAttributes)
                .withJavaObject(modifiedCount) // Body contains modified count.
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

    public void setDocument(DynamicObject document) {
        this.document = document;
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
