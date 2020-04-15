package com.reedelk.mongodb.component;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.fail;
import static org.testcontainers.shaded.com.google.common.collect.ImmutableMap.of;

@Testcontainers
@ExtendWith(MockitoExtension.class)
class FindTest {

    @Mock
    private FlowContext context;


    @Container
    public GenericContainer<?> mongodb = new GenericContainer<>("mongo:4.0.4")
            .withExposedPorts(27017);

    private Find component = new Find();
    private String connectionURL;
    private String database;

    @BeforeEach
    void setUp() {
        String containerIpAddress = mongodb.getContainerIpAddress();
        Integer firstMappedPort = mongodb.getFirstMappedPort();
        this.connectionURL = "mongodb://" + containerIpAddress + ":" + firstMappedPort;
        this.database = "test-database";
        ConnectionConfiguration connectionConfiguration = new ConnectionConfiguration();
        connectionConfiguration.setConnectionURL(connectionURL);
        connectionConfiguration.setDatabase(database);
        component.setConnection(connectionConfiguration);
        component.clientFactory = new ClientFactory();
    }

    @AfterEach
    void tearDown() {
        if (component != null) component.dispose();
    }

    @Test
    void shouldCorrectlyFindItemsWithoutFilter() {
        // Given
        String collectionName = "test-collection";
        insert(connectionURL, database, collectionName, "{name:'Olav', surname: 'Zipser'}");
        insert(connectionURL, database, collectionName, "{name:'Mark', surname: 'Anton'}");
        component.setCollection(collectionName);
        component.initialize();

        Message message = MessageBuilder.get().empty().build();

        // When
        Message actual = component.apply(context, message);

        // Then
        List<Map<String, Object>> results = actual.payload();

        assertExistEntry(results, of("name", "Olav", "surname", "Zipser"));
        assertExistEntry(results, of("name", "Mark", "surname", "Anton"));
    }

    private void assertExistEntry(List<Map<String, Object>> results, Map<String, Object> expected) {
        for (Map<String, Object> result : results) {
            if (match(result, expected)) return;
        }
        fail("Could not find matching item");
    }

    private boolean match(Map<String,Object> actual, Map<String, Object> expected) {
        if (actual.size() -1 != expected.size()) return false; // We exclude _id which is randomly generated.
        return actual.entrySet().stream()
                .filter(stringObjectEntry -> !stringObjectEntry.getKey().equals("_id"))
                .allMatch(e -> e.getValue().equals(expected.get(e.getKey())));
    }

    private void insert(String connectionURL, String database, String collection, String data) {
        MongoClient client = MongoClients.create(connectionURL);
        MongoDatabase mongoDatabase = client.getDatabase(database);
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);
        mongoCollection.insertOne(Document.parse(data));
        client.close();
    }
}