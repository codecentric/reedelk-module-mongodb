package com.reedelk.mongodb.component;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.mongodb.internal.commons.DocumentUtils;
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
import com.reedelk.runtime.api.script.dynamicvalue.DynamicObject;
import org.bson.Document;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ServiceScope;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotBlank;

@ModuleComponent("MongoDB Update (One/Many)")
@Component(service = Update.class, scope = ServiceScope.PROTOTYPE)
public class Update implements ProcessorSync {

    @Property("Connection")
    private ConnectionConfiguration connection;

    @Property("Collection")
    private String collection;

    @Property("Find Filter")
    @InitValue("#[message.payload()]")
    @DefaultValue("#[message.payload()")
    private DynamicObject findFilter;

    @Property("Updated Document or Filter")
    @InitValue("#[message.payload()]")
    @DefaultValue("#[message.payload()")
    private DynamicObject document;

    @Property("Update Many")
    private Boolean many;

    @Reference
    private ScriptEngineService scriptService;
    @Reference
    private ClientFactory clientFactory;

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

        Object filter = scriptService.evaluate(findFilter, flowContext, message)
                .orElseThrow(() -> new PlatformException("Find filter"));

        Object toUpdate = scriptService.evaluate(document, flowContext, message)
                .orElseThrow(() -> new PlatformException("Updated document"));


        UpdateResult updateResult;

        // MongoDB Pipeline
        if (toUpdate instanceof List) {
            // The to update document is a pipeline.
            Document toUpdateFilter = DocumentUtils.from(filter);
            List<Object> toUpdateList = (List<Object>) toUpdate;
            List<Document> toUpdateDocuments = new ArrayList<>();
            for (Object list : toUpdateList) {
                toUpdateDocuments.add(DocumentUtils.from(list));
            }
            if (many != null && many) {
                updateResult = mongoCollection.updateMany(toUpdateFilter, toUpdateDocuments);
            } else {
                updateResult = mongoCollection.updateOne(toUpdateFilter, toUpdateDocuments);
            }

            // MongoDB Update
        } else {
            Document toUpdateFilter = DocumentUtils.from(filter);
            Document toUpdateDocument = DocumentUtils.from(toUpdate);
            if (many != null && many) {
                updateResult = mongoCollection.updateMany(toUpdateFilter, toUpdateDocument);
            } else {
                updateResult = mongoCollection.updateOne(toUpdateFilter, toUpdateDocument);
            }
        }

        long matchedCount = updateResult.getMatchedCount();
        long modifiedCount = updateResult.getModifiedCount();
        String upsertedId = updateResult.getUpsertedId().toString();

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

    public void setFindFilter(DynamicObject findFilter) {
        this.findFilter = findFilter;
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
