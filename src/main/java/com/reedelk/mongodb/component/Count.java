package com.reedelk.mongodb.component;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.mongodb.internal.commons.DocumentUtils;
import com.reedelk.mongodb.internal.commons.Unsupported;
import com.reedelk.mongodb.internal.exception.MongoDBCountException;
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

import static com.reedelk.mongodb.internal.commons.Messages.Count.COUNT_QUERY_NULL;
import static com.reedelk.runtime.api.commons.ComponentPrecondition.Configuration.requireNotBlank;
import static com.reedelk.runtime.api.commons.DynamicValueUtils.isNotNullOrBlank;

@ModuleComponent("MongoDB Count")
@Component(service = Count.class, scope = ServiceScope.PROTOTYPE)
@Description("Counts the documents from the given database collection using the configured connection. " +
        "The connection configuration allows to specify host, port, database name, username and password to be used for authentication against the database. " +
        "If the query is not empty, only the documents matching the query filter will be taken in consideration by the count.")
public class Count implements ProcessorSync {

    @DialogTitle("MongoDB Connection")
    @Property("Connection")
    @Description("MongoDB connection configuration to be used by this count operation. " +
            "Shared configurations use the same MongoDB client.")
    private ConnectionConfiguration connection;

    @Property("Collection")
    @Hint("MyCollection")
    @Example("MyCollection")
    @Description("Sets the name of the collection to be used for the count operation.")
    private String collection;

    @Property("Query Filter")
    @Hint("{ name: 'John' }")
    @Example("{ age: 35 } ")
    @Description("Sets the query filter to be applied to the count operation. " +
            "If no query is present all the documents from the given collections will be counted.")
    private DynamicObject query;

    @Reference
    ScriptEngineService scriptService;
    @Reference
    ClientFactory clientFactory;

    private MongoClient client;

    @Override
    public void initialize() {
        requireNotBlank(Delete.class, collection, "MongoDB collection must not be empty");
        this.client = clientFactory.clientByConfig(this, connection);
    }

    @Override
    public Message apply(FlowContext flowContext, Message message) {

        MongoDatabase mongoDatabase = client.getDatabase(connection.getDatabase());
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);

        long count;

        if (isNotNullOrBlank(query)) {

            Object evaluatedQuery = scriptService.evaluate(query, flowContext, message)
                    .orElseThrow(() -> new MongoDBCountException(COUNT_QUERY_NULL.format(query.value())));

            Document countQuery = DocumentUtils.from(evaluatedQuery, Unsupported.queryType(evaluatedQuery));

            count = mongoCollection.countDocuments(countQuery);

        } else {
            count = mongoCollection.countDocuments();
        }

        return MessageBuilder.get(Count.class)
                .withJavaObject(count)
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

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public void setQuery(DynamicObject query) {
        this.query = query;
    }
}
