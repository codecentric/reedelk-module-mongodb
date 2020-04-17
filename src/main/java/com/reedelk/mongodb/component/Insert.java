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
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicvalue.DynamicObject;
import org.bson.Document;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ServiceScope;

import java.util.List;
import java.util.stream.Collectors;

import static com.reedelk.mongodb.internal.commons.Messages.Insert.INSERT_DOCUMENT_EMPTY;
import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotBlank;

@ModuleComponent("MongoDB Insert (One/Many)")
@Component(service = Insert.class, scope = ServiceScope.PROTOTYPE)
@Description("Inserts one or more document into a MongoDB database on the specified collection. " +
        "The MongoDB connection configuration allows to specify host, port, database, username and password to be used for the MongoDB connection. " +
        "The input document can be a static or a dynamic expression. By default the message payload " +
        "is used as a document to be inserted. The input document could be a JSON string, " +
        "Map, Pair or DataRow (Insert One). " +
        "If the input is a list every item in the list will be considered a document to " +
        "be inserted and all the documents in the list will be inserted in batch (Insert Many). ")
public class Insert implements ProcessorSync {

    private static final long ONE = 1L;
    private static final long ZERO = 0L;

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
    @Description("Sets the document to be inserted into the database. " +
            "The input document can be a static or a dynamic expression. " +
            "The input document could be a JSON string, Map, Pair or DataRow (Insert One). " +
            "If the input is a list every item in the list will be considered a document to " +
            "be inserted and all the documents in the list will be inserted in batch (Insert Many).")
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
            return insertMany(mongoCollection, (List<Object>) insertDocument);
        } else {
            return insertOne(mongoCollection, insertDocument);
        }
    }

    @Override
    public void dispose() {
        clientFactory.dispose(this, connection);
        client = null;
    }

    private Message insertMany(MongoCollection<Document> mongoCollection, List<Object> toInsertList) {
        if (toInsertList.isEmpty()) {
            return MessageBuilder.get(Insert.class)
                    .withJavaObject(ZERO) // The payload contains the number of documents inserted.
                    .build();
        }

        List<Document> toInsertDocuments = toInsertList
                .stream()
                .map(DocumentUtils::from)
                .collect(Collectors.toList());

        mongoCollection.insertMany(toInsertDocuments);

        // The payload body contains the number of inserted documents.
        // In this case it is always one.
        return MessageBuilder.get(Insert.class)
                .withJavaObject((long) toInsertDocuments.size()) // The payload contains the number of documents inserted.
                .build();
    }

    private Message insertOne(MongoCollection<Document> mongoCollection, Object insertDocument) {
        // Insert One Document
        Document documentToInsert = DocumentUtils.from(insertDocument);
        mongoCollection.insertOne(documentToInsert);

        // The payload body contains the number of inserted documents.
        // In this case it is always one.
        return MessageBuilder.get(Insert.class)
                .withJavaObject(ONE) // The payload contains the number of documents inserted.
                .build();
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
