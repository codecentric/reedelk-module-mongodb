package com.reedelk.mongodb.component;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.mongodb.internal.commons.DocumentUtils;
import com.reedelk.runtime.api.annotation.DefaultValue;
import com.reedelk.runtime.api.annotation.InitValue;
import com.reedelk.runtime.api.annotation.ModuleComponent;
import com.reedelk.runtime.api.annotation.Property;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.exception.PlatformException;
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

import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotBlank;

@ModuleComponent("MongoDB Insert (One/Many)")
@Component(service = Insert.class, scope = ServiceScope.PROTOTYPE)
public class Insert implements ProcessorSync {

    @Property("Connection")
    private ConnectionConfiguration connection;

    @Property("Collection")
    private String collection;

    @Property("Insert Document")
    @InitValue("#[message.payload()]")
    @DefaultValue("#[message.payload()")
    private DynamicObject document;

    @Reference
    private ScriptEngineService scriptService;
    @Reference
    private ClientFactory clientFactory;

    private MongoClient client;

    @Override
    public void initialize() {
        requireNotBlank(Insert.class, collection, "MongoDB collection must not be empty");
        this.client = clientFactory.clientByConfig(this, connection);
    }

    @Override
    public Message apply(FlowContext flowContext, Message message) {

        MongoDatabase mongoDatabase = client.getDatabase(connection.getDatabase());

        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);

        Object insertDocument = scriptService.evaluate(document, flowContext, message)
                .orElseThrow(() -> new PlatformException("Insert document"));

        if (insertDocument instanceof List) {
            // Insert many
            List<Object> toInsertList = (List<Object>) insertDocument;
            List<Document> toInsertDocuments = new ArrayList<>();
            for (Object list : toInsertList) {
                toInsertDocuments.add(DocumentUtils.from(list));
            }
            mongoCollection.insertMany(toInsertDocuments);

        } else {
            Document documentToInsert = DocumentUtils.from(insertDocument);

            mongoCollection.insertOne(documentToInsert);
        }

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

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public void setDocument(DynamicObject document) {
        this.document = document;
    }
}
