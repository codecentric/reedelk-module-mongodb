package com.reedelk.mongodb.component;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.mongodb.internal.commons.DocumentUtils;
import com.reedelk.mongodb.internal.commons.ObjectIdReplacer;
import com.reedelk.mongodb.internal.exception.MongoDBFindException;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.message.content.MimeType;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicvalue.DynamicObject;
import org.bson.Document;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ServiceScope;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.reedelk.mongodb.internal.commons.Messages.Find.FIND_FILTER_NULL;
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

    @Property("Out mime type")
    @DefaultValue(MimeType.AsString.APPLICATION_JSON)
    @Combo(comboValues = {
            MimeType.AsString.APPLICATION_JSON,
            MimeType.AsString.APPLICATION_JAVA})
    @Description("Sets the mime type of the output. If output is application/json, " +
            "then the content is a JSON String containing the results of the find operation. " +
            "Use this output type if you don't need to further process the result. If output is application/java, " +
            "the output is a List of Map which can be used righ away from the script language to do further processing " +
            "of the results.")
    private String mimeType;

    @Reference
    ScriptEngineService scriptService;
    @Reference
    ClientFactory clientFactory;

    private MongoClient client;

    @Override
    public void initialize() {
        requireNotBlank(Find.class, collection, "MongoDB collection must not be empty");
        this.client = clientFactory.clientByConfig(this, connection);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Message apply(FlowContext flowContext, Message message) {

        MongoDatabase mongoDatabase = client.getDatabase(connection.getDatabase());
        MongoCollection<Document> mongoDatabaseCollection = mongoDatabase.getCollection(collection);

        FindIterable<Document> documents;

        if (isNotNullOrBlank(filter)) {
            // Find documents matching the given filter. The filter could be a JSON
            // string, a Map or a Pair type. If the filter is not one of these objects
            // we throw an exception.
            Object evaluatedFilter = scriptService.evaluate(this.filter, flowContext, message)
                    .orElseThrow(() -> new MongoDBFindException(FIND_FILTER_NULL.format(this.filter.value())));

            Document documentFilter = DocumentUtils.from(evaluatedFilter);
            documents = mongoDatabaseCollection.find(documentFilter);

        } else {
            // Filter was not given, we find all the documents in the collection.
            documents = mongoDatabaseCollection.find();
        }

        MimeType parsedMimeType = MimeType.parse(this.mimeType, MimeType.APPLICATION_JSON);

        if (MimeType.APPLICATION_JSON.equals(parsedMimeType)) {
            // application/json -> String
            List<String> output = new ArrayList<>();
            documents.forEach((Consumer<Document>) document -> {
                ObjectIdReplacer.replace(document);
                output.add(document.toJson());
            });
            return MessageBuilder.get(Find.class)
                    .withJson(output.toString())
                    .build();

        } else {
            // application/java -> Map or List
            List<Map> output = new ArrayList<>();
            documents.forEach((Consumer<Document>) document -> {
                ObjectIdReplacer.replace(document);
                Map<String, Object> wrapped = new HashMap<>(document); // We wrap it so that it uses the to string of java.Map instead of Document.
                output.add(wrapped);
            });
            return MessageBuilder.get(Find.class)
                    .withList(output, Map.class)
                    .build();
        }
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

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }
}
