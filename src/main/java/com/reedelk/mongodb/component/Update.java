package com.reedelk.mongodb.component;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.mongodb.internal.commons.DocumentUtils;
import com.reedelk.mongodb.internal.commons.Utils;
import com.reedelk.mongodb.internal.exception.MongoDBUpdateException;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.commons.ImmutableMap;
import com.reedelk.runtime.api.component.ProcessorSync;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotBlank;

@ModuleComponent("MongoDB Update (One/Many)")
@Component(service = Update.class, scope = ServiceScope.PROTOTYPE)
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

    @SuppressWarnings("unchecked")
    @Override
    public Message apply(FlowContext flowContext, Message message) {

        MongoDatabase mongoDatabase = client.getDatabase(connection.getDatabase());
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);

        Object filter = scriptService.evaluate(this.filter, flowContext, message)
                .orElseThrow(() -> new MongoDBUpdateException("Find filter"));

        Object toUpdate = scriptService.evaluate(document, flowContext, message)
                .orElseThrow(() -> new MongoDBUpdateException("Updated document"));


        UpdateResult updateResult;

        // Update with pipeline
        if (toUpdate instanceof List) {
            // The to update document is a pipeline.
            Document toUpdateFilter = DocumentUtils.from(filter);
            List<Object> toUpdateList = (List<Object>) toUpdate;
            List<Document> toUpdateDocuments = new ArrayList<>();
            for (Object list : toUpdateList) {
                toUpdateDocuments.add(DocumentUtils.from(list));
            }
            if (Utils.isTrue(many)) {
                updateResult = mongoCollection.updateMany(toUpdateFilter, toUpdateDocuments);
            } else {
                updateResult = mongoCollection.updateOne(toUpdateFilter, toUpdateDocuments);
            }

        } else {
            // Update
            Document toUpdateFilter = DocumentUtils.from(filter);
            Document toUpdateDocument = DocumentUtils.from(toUpdate);
            if (Utils.isTrue(many)) {
                updateResult = mongoCollection.updateMany(toUpdateFilter, toUpdateDocument);
            } else {
                updateResult = mongoCollection.updateOne(toUpdateFilter, toUpdateDocument);
            }
        }

        long matchedCount = updateResult.getMatchedCount();
        long modifiedCount = updateResult.getModifiedCount();
        String upsertedId = Optional.ofNullable(updateResult.getUpsertedId())
                .map(Object::toString)
                .orElse(null);

        Map<String, Serializable> componentAttributes = ImmutableMap.of(
                "matchedCount", matchedCount,
                "modifiedCount", modifiedCount,
                "upsertedId", upsertedId);

        MessageAttributes attributes = new DefaultMessageAttributes(Delete.class, componentAttributes);

        return MessageBuilder.get()
                .attributes(attributes)
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

    public void setFilter(DynamicObject filter) {
        this.filter = filter;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public void setDocument(DynamicObject document) {
        this.document = document;
    }

    public void setMany(Boolean many) {
        this.many = many;
    }
}
