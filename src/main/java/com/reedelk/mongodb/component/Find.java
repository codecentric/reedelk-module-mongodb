package com.reedelk.mongodb.component;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.mongodb.internal.attribute.FindAttributes;
import com.reedelk.mongodb.internal.commons.DocumentUtils;
import com.reedelk.mongodb.internal.commons.ObjectIdUtils;
import com.reedelk.mongodb.internal.commons.Unsupported;
import com.reedelk.mongodb.internal.exception.MongoDBFindException;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.converter.ConverterService;
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

import static com.reedelk.mongodb.internal.commons.Messages.Find.FIND_QUERY_NULL;
import static com.reedelk.runtime.api.commons.ComponentPrecondition.Configuration.requireNotBlank;
import static com.reedelk.runtime.api.commons.DynamicValueUtils.isNotNullOrBlank;

@ModuleComponent("MongoDB Find")
@ComponentOutput(
        attributes = FindAttributes.class,
        payload = { List.class, String.class },
        description = "A list of Objects representing the documents found or a JSON string with the documents found if the output mime type was application/json.")
@ComponentInput(
        payload = Object.class,
        description = "The input payload is used to evaluate the query filter expression.")
@Component(service = Find.class, scope = ServiceScope.PROTOTYPE)
@Description("Finds one or more documents from the specified database collection. " +
        "The connection configuration allows to specify host, port, database name, username and password to be used for authentication against the database. " +
        "A static or dynamic query filter can be applied to the find operation to filter the results. " +
        "This component allows to specify the mime type of the output. " +
        "If you need to further process the result set in a script, it is recommended to output 'application/java' " +
        "in order to avoid further conversion from JSON to Object. If you need the result as is, then keep " +
        "'application/json' as output mime type.")
public class Find implements ProcessorSync {

    @DialogTitle("MongoDB Connection")
    @Property("Connection")
    @Description("MongoDB connection configuration to be used by this find operation. " +
            "Shared configurations use the same MongoDB client.")
    private ConnectionConfiguration connection;

    @Property("Collection")
    @Hint("MyCollection")
    @Example("MyCollection")
    @Description("Sets the name of the collection to be used for the find operation.")
    private String collection;

    @Property("Query Filter")
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
    @Description("Sets the query filter to be applied to the find operation. " +
            "If no filter is present all the documents from the given collection will be retrieved.")
    private DynamicObject query;

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
    ConverterService converterService;
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

        FindAttributes attributes;

        if (isNotNullOrBlank(query)) {
            // Find documents matching the given filter. The filter could be a JSON
            // string, a Map or a Pair type. If the filter is not one of these objects
            // we throw an exception.
            Object evaluatedQuery = scriptService.evaluate(query, flowContext, message)
                    .orElseThrow(() -> new MongoDBFindException(FIND_QUERY_NULL.format(query.value())));

            Document findQuery = DocumentUtils.from(converterService, evaluatedQuery, Unsupported.queryType(evaluatedQuery));
            documents = mongoDatabaseCollection.find(findQuery);

            attributes = new FindAttributes(collection, evaluatedQuery);

        } else {
            // Filter was not given, we find all the documents in the collection.
            documents = mongoDatabaseCollection.find();

            attributes = new FindAttributes(collection, null);
        }

        MimeType parsedMimeType = MimeType.parse(this.mimeType, MimeType.APPLICATION_JSON);

        // The output message depends on the wanted mime type.
        if (MimeType.APPLICATION_JSON.equals(parsedMimeType)) {
            // application/json -> String
            List<String> output = new ArrayList<>();
            documents.forEach((Consumer<Document>) document ->
                    output.add(document.toJson()));
            return MessageBuilder.get(Find.class)
                    .withJson(output.toString())
                    .attributes(attributes)
                    .build();

        } else {
            // application/java -> Map or List
            List<Map> output = new ArrayList<>();
            documents.forEach((Consumer<Document>) document -> {
                ObjectIdUtils.replace(document);
                Map<String, Object> wrapped = new HashMap<>(document); // We wrap it so that it uses the to string of java.Map instead of Document.
                output.add(wrapped);
            });
            return MessageBuilder.get(Find.class)
                    .withList(output, Map.class)
                    .attributes(attributes)
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

    public void setQuery(DynamicObject query) {
        this.query = query;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }
}
