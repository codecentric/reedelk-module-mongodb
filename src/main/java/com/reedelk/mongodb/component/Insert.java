package com.reedelk.mongodb.component;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.mongodb.internal.commons.DocumentUtils;
import com.reedelk.mongodb.internal.exception.MongoDBInsertException;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicvalue.DynamicObject;
import org.bson.Document;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ServiceScope;

import java.util.ArrayList;
import java.util.List;

import static com.reedelk.mongodb.internal.commons.Messages.Insert.INSERT_DOCUMENT_EMPTY;
import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotBlank;

@ModuleComponent("MongoDB Insert (One/Many)")
@Component(service = Insert.class, scope = ServiceScope.PROTOTYPE)
public class Insert implements ProcessorSync {

    @Property("Connection")
    @Description("MongoDB connection configuration to be used by this insert operation. " +
            "Shared configurations use the same MongoDB client.")
    private ConnectionConfiguration connection;

    @Property("Collection")
    @Hint("MyCollection")
    @Example("MyCollection")
    @Description("Sets the name of the MongoDB collection to be used for the insert operation.")
    private String collection;

    @Property("Insert Document")
    @InitValue("#[message.payload()]")
    @DefaultValue("#[message.payload()")
    private DynamicObject document;

    @Reference
    ScriptEngineService scriptService;
    @Reference
    ClientFactory clientFactory;

    private MongoClient client;

    @Override
    public void initialize() {
        requireNotBlank(Insert.class, collection, "MongoDB collection must not be empty");
        this.client = clientFactory.clientByConfig(this, connection);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Message apply(FlowContext flowContext, Message message) {

        MongoDatabase mongoDatabase = client.getDatabase(connection.getDatabase());

        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);

        Object insertDocument = scriptService.evaluate(document, flowContext, message)
                .orElseThrow(() -> new MongoDBInsertException(INSERT_DOCUMENT_EMPTY.format(document.value())));

        if (insertDocument instanceof List) {
            // Insert Many Documents
            List<Object> toInsertList = (List<Object>) insertDocument;
            if (toInsertList.isEmpty()) {
                return message;
            }

            List<Document> toInsertDocuments = new ArrayList<>();
            for (Object list : toInsertList) {
                toInsertDocuments.add(DocumentUtils.from(list));
            }
            mongoCollection.insertMany(toInsertDocuments);

        } else {
            // Insert One Document
            Document documentToInsert = DocumentUtils.from(insertDocument);
            mongoCollection.insertOne(documentToInsert);
        }

        // TODO: Attributes?
        return message;
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
}
