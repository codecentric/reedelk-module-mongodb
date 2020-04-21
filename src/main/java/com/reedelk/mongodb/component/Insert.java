package com.reedelk.mongodb.component;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.mongodb.internal.commons.DocumentUtils;
import com.reedelk.mongodb.internal.commons.ObjectIdUtils;
import com.reedelk.mongodb.internal.commons.Unsupported;
import com.reedelk.mongodb.internal.exception.MongoDBDocumentException;
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

import java.util.Collections;
import java.util.List;

import static com.reedelk.mongodb.internal.commons.Messages.Insert.INSERT_DOCUMENT_EMPTY;
import static com.reedelk.mongodb.internal.commons.Utils.evaluateOrUsePayloadWhenEmpty;
import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotBlank;
import static java.util.stream.Collectors.toList;

@ModuleComponent("MongoDB Insert (One/Many)")
@Component(service = Insert.class, scope = ServiceScope.PROTOTYPE)
@Description("Inserts one or more documents into the given database collection. " +
        "The connection configuration allows to specify host, port, database name, username and password to be used for authentication against the database. " +
        "The input document can be a static or a dynamic expression. By default the message payload " +
        "is used as a document to be inserted. The input document could be a JSON string, " +
        "a Map, a Pair or a DataRow (Insert One). " +
        "If the input is a list every item in the list will be considered as a separate document " +
        "and all the documents in the list will be inserted (Insert Many). ")
public class Insert implements ProcessorSync {

    @Property("Connection")
    @Description("MongoDB connection configuration to be used by this insert operation. " +
            "Shared configurations use the same MongoDB client.")
    private ConnectionConfiguration connection;

    @Property("Collection")
    @Hint("MyCollection")
    @Example("MyCollection")
    @Description("Sets the name of the collection to be used for the insert operation.")
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

        // TODO: One component must only have one Input type. Here you might have list and not list
        //  therefore there should be InsertOne and InsertMany.
        Object insertDocument = evaluateOrUsePayloadWhenEmpty(document, scriptService, flowContext, message,
                () -> new MongoDBDocumentException(INSERT_DOCUMENT_EMPTY.format(document.value())));

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
                    .withJavaObject(Collections.emptyList())
                    .build();
        }

        List<Document> toInsertDocuments = toInsertList
                .stream()
                .map(documentAsObject -> DocumentUtils.from(documentAsObject, Unsupported.documentType(documentAsObject)))
                .collect(toList());

        mongoCollection.insertMany(toInsertDocuments);

        List<Object> insertIds = toInsertDocuments.stream()
                .map(document -> document.get(ObjectIdUtils.OBJECT_ID_PROPERTY))
                .map(ObjectIdUtils::replace)
                .collect(toList());

        // The payload body contains the number of inserted documents.
        // In this case it is always one.
        return MessageBuilder.get(Insert.class)
                .withJavaObject(insertIds) // The payload contains the IDs of the inserted documents.
                .build();
    }

    private Message insertOne(MongoCollection<Document> mongoCollection, Object insertDocument) {
        // Insert One Document
        Document documentToInsert = DocumentUtils.from(insertDocument, Unsupported.documentType(insertDocument));
        mongoCollection.insertOne(documentToInsert);

        Object insertId = documentToInsert.get(ObjectIdUtils.OBJECT_ID_PROPERTY);

        // The payload body contains the number of inserted documents.
        // In this case it is always one.
        return MessageBuilder.get(Insert.class)
                .withJavaObject(ObjectIdUtils.replace(insertId)) // The payload contains the id of inserted document.
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
