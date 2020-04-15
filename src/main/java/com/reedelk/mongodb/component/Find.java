package com.reedelk.mongodb.component;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.mongodb.internal.commons.DocumentUtils;
import com.reedelk.runtime.api.annotation.*;
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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotBlank;
import static com.reedelk.runtime.api.commons.DynamicValueUtils.isNotNullOrBlank;

@ModuleComponent("MongoDB Find")
@Component(service = Find.class, scope = ServiceScope.PROTOTYPE)
public class Find implements ProcessorSync {

    @Property("Connection")
    @Description("MongoDB connection configuration to be used by this find operation. " +
            "Shared configurations use the same MongoDB client.")
    private ConnectionConfiguration connection;

    @Property("Collection")
    @Hint("MyCollection")
    @Example("MyCollection")
    @Description("Sets the name of the MongoDB collection to be used for the find operation.")
    private String collection;

    @Property("Find Filter")
    @Hint("{ \"name.last\": \"Hopper\" }")
    @Example("<ul>" +
            "<li>{ _id: 5 }</li>" +
            "<li>{ \"name.last\": \"Hopper\" }</li>" +
            "<li>{ _id: { $in: [ 5, ObjectId(\"507c35dd8fada716c89d0013\") ] } }</li>" +
            "<li>{ birth: { $gt: new Date('1950-01-01') } }</li>" +
            "<li>{ \"name.last\": { $regex: /^N/ } }</li>" +
            "<li>{\n" +
            "   birth: { $gt: new Date('1920-01-01') },\n" +
            "   death: { $exists: false }\n" +
            "}</li>" +
            "<li><code>context.myFindFilter</code></li>" +
            "</ul>")
    @Description("Sets the filter to be applied to the find operation. " +
            "If no filter is present all the documents from the given collection will be retrieved.")
    private DynamicObject filter;

    @Reference
    private ScriptEngineService scriptService;
    @Reference
    private ClientFactory clientFactory;

    private MongoClient client;

    @Override
    public void initialize() {
        requireNotBlank(Find.class, collection, "MongoDB collection must not be empty");
        this.client = clientFactory.clientByConfig(this, connection);
    }

    @Override
    public Message apply(FlowContext flowContext, Message message) {

        MongoDatabase mongoDatabase = client.getDatabase(connection.getDatabase());

        MongoCollection<Document> mongoDatabaseCollection = mongoDatabase.getCollection(collection);

        FindIterable<Document> documents;

        if (isNotNullOrBlank(filter)) {
            // Find documents matching find filter
            Object filter = scriptService.evaluate(this.filter, flowContext, message)
                    .orElseThrow(() -> new PlatformException("Find filter was null or empty"));

            Document documentFilter = DocumentUtils.from(filter);

            // Find one with filter
            documents = mongoDatabaseCollection.find(documentFilter);

        } else {
            // Find all (no filter was provided)
            documents = mongoDatabaseCollection.find();
        }

        List<String> output = new ArrayList<>();
        documents.forEach((Consumer<Document>) document -> output.add(document.toJson()));

        return MessageBuilder.get()
                .withList(output, String.class)
                .build();
    }

    @Override
    public void dispose() {
        clientFactory.dispose(this, connection);
        client = null;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public void setFilter(DynamicObject filter) {
        this.filter = filter;
    }

    public void setConnection(ConnectionConfiguration connection) {
        this.connection = connection;
    }
}
