package com.reedelk.mongodb.component;

import com.mongodb.client.*;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicvalue.DynamicValue;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.*;

@Testcontainers
@ExtendWith(MockitoExtension.class)
abstract class AbstractMongoDBTest {

    @Container
    public static GenericContainer<?> mongodb = new GenericContainer<>("mongo:4.0.4")
            .withExposedPorts(27017);

    @Mock
    protected FlowContext context;
    @Mock
    protected ScriptEngineService scriptService;

    private static String database;
    private static String connectionURL;

    protected static final String collectionName = "test-collection";
    protected ConnectionConfiguration connectionConfiguration;

    @BeforeAll
    protected static void setUpAll() {
        String containerIpAddress = mongodb.getContainerIpAddress();
        Integer firstMappedPort = mongodb.getFirstMappedPort();

        database = "test-database";
        connectionURL = "mongodb://" + containerIpAddress + ":" + firstMappedPort;
    }

    @AfterAll
    protected static void afterAll() {
        removeAllDocuments();
    }

    @BeforeEach
    void setUp() {
        connectionConfiguration = new ConnectionConfiguration();
        connectionConfiguration.setConnectionURL(connectionURL);
        connectionConfiguration.setDatabase(database);

        lenient().doAnswer(invocation -> {
            DynamicValue<?> argument = invocation.getArgument(0);
            return Optional.ofNullable(argument.value());
        }).when(scriptService).evaluate(any(DynamicValue.class), eq(context), any(Message.class));
    }

    @AfterEach
    void tearDown() {
        removeAllDocuments();
    }

    protected void assertExistEntry(List<Map<String, Object>> results, Map<String, Object> expected) {
        for (Map<String, Object> result : results) {
            if (match(result, expected)) return;
        }
        fail("Could not find matching item");
    }

    protected boolean match(Map<String,Object> actual, Map<String, Object> expected) {
        if (actual.size() -1 != expected.size()) return false; // We exclude _id which is randomly generated.
        return actual.entrySet().stream()
                .filter(stringObjectEntry -> !stringObjectEntry.getKey().equals("_id"))
                .allMatch(e -> e.getValue().equals(expected.get(e.getKey())));
    }

    protected static void insertDocument(String document) {
        MongoClient client = MongoClients.create(connectionURL);
        MongoDatabase mongoDatabase = client.getDatabase(database);
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collectionName);
        mongoCollection.insertOne(Document.parse(document));
        client.close();
    }

    protected static void assertExistDocumentWith(String filter) {
        MongoClient client = MongoClients.create(connectionURL);
        MongoDatabase mongoDatabase = client.getDatabase(database);
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collectionName);
        FindIterable<Document> documents = mongoCollection.find(Document.parse(filter));
        assertThat(documents)
                .withFailMessage("Could not find document with filter=[" + filter + "]")
                .hasSize(1);
        client.close();
    }

    protected static void assertDocumentsCount(int expectedCount) {
        MongoClient client = MongoClients.create(connectionURL);
        MongoDatabase mongoDatabase = client.getDatabase(database);
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collectionName);
        long count = mongoCollection.countDocuments();
        assertThat(count).isEqualTo(expectedCount);
        client.close();
    }

    protected static void assertExistDocumentsWith(String filter, int expectedDocuments) {
        MongoClient client = MongoClients.create(connectionURL);
        MongoDatabase mongoDatabase = client.getDatabase(database);
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collectionName);
        FindIterable<Document> documents = mongoCollection.find(Document.parse(filter));
        assertThat(documents)
                .withFailMessage("Could not find ("+ expectedDocuments + ") documents with filter=[" + filter + "]")
                .hasSize(expectedDocuments);
        client.close();
    }

    protected static void removeAllDocuments() {
        MongoClient client = MongoClients.create(connectionURL);
        MongoDatabase mongoDatabase = client.getDatabase(database);
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collectionName);
        mongoCollection.deleteMany(Document.parse("{}"));
        long count = mongoCollection.countDocuments();
        assertThat(count).isEqualTo(0);
        client.close();
    }
}
