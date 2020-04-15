package com.reedelk.mongodb.component;

import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.message.content.Pair;
import com.reedelk.runtime.api.script.dynamicvalue.DynamicObject;
import org.junit.jupiter.api.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.*;
import static org.testcontainers.shaded.com.google.common.collect.ImmutableMap.of;

class InsertTest extends AbstractMongoDBTest {

    private Insert component = new Insert();
    private static String collectionName = "test-collection";

    @BeforeAll
    public static void setUpAll() {
        AbstractMongoDBTest.setUpAll();
    }

    @AfterAll
    public static void afterAll() {
        removeAllDocuments(collectionName);
    }

    @BeforeEach
    void setUp() {
        super.setUp();
        component.setConnection(connectionConfiguration);
        component.setCollection(collectionName);
        component.clientFactory = new ClientFactory();
        component.scriptService = scriptService;
    }

    @AfterEach
    void tearDown() {
        removeAllDocuments(collectionName);
    }

    @Test
    void shouldInsertDocumentFromJsonString() {
        // Given
        String documentAsJson = "{name: 'John', surname: 'Doe', age: 23 }";
        component.setDocument(DynamicObject.from(documentAsJson));
        component.initialize();
        Message input = MessageBuilder.get().empty().build();

        // When
        component.apply(context, input);

        // Then
        assertExistDocumentWith(collectionName, "{ name: 'John' }");
    }

    @Test
    void shouldInsertDocumentFromMap() {
        // Given
        Map<String, Serializable> documentMap =
                of("name", "John", "surname", "Doe", "age", 23);
        component.setDocument(DynamicObject.from(documentMap));
        component.initialize();

        Message input = MessageBuilder.get().empty().build();

        // When
        component.apply(context, input);

        // Then
        assertExistDocumentWith(collectionName, "{ age: 23 }");
    }

    @Test
    void shouldInsertDocumentFromPair() {
        // Given
        Pair<String, Serializable> documentPair = Pair.create("name", "John");
        component.setDocument(DynamicObject.from(documentPair));
        component.initialize();

        Message input = MessageBuilder.get().empty().build();

        // When
        component.apply(context, input);

        // Then
        assertExistDocumentWith(collectionName, "{ name: 'John' }");
    }

    @Test
    void shouldInsertDocumentsFromListOfJsons() {
        // Given
        String documentAsJson1 = "{name: 'John', surname: 'Doe', age: 45 }";
        String documentAsJson2 = "{name: 'Anton', surname: 'Ellis', age: 23 }";
        String documentAsJson3 = "{name: 'Olav', surname: 'Zipser', age: 65 }";
        List<String> documents = asList(documentAsJson1, documentAsJson2, documentAsJson3);

        component.setDocument(DynamicObject.from(documents));
        component.initialize();
        Message input = MessageBuilder.get().empty().build();

        // When
        component.apply(context, input);

        // Then
        assertExistDocumentWith(collectionName, "{ name: 'John' }");
        assertExistDocumentWith(collectionName, "{ surname: 'Ellis' }");
        assertExistDocumentWith(collectionName, "{ age: 65 }");
        assertExistDocumentsWith(collectionName, "{ age: { $gt: 30 } }", 2);
    }
}