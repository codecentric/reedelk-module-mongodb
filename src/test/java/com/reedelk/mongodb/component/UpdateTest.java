package com.reedelk.mongodb.component;

import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.script.dynamicvalue.DynamicObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class UpdateTest extends AbstractMongoDBTest {

    private Update component = new Update();

    @BeforeAll
    public static void setUpAll() {
        AbstractMongoDBTest.setUpAll();
        insertDocument("{name:'Olav', surname: 'Zipser', age: 55}");
        insertDocument("{name:'Mark', surname: 'Anton', age: 32}");
    }

    @BeforeEach
    void setUp() {
        super.setUp();
        component.setConnection(connectionConfiguration);
        component.setCollection(collectionName);
        component.clientFactory = new ClientFactory();
        component.scriptService = scriptService;
    }

    @Test
    void shouldUpdateCorrectly() {
        // Given
        String filterAsJson = "{ name: 'Olav' }";
        String updatedDocumentAsJson = "{ $set: { name: 'Josh', surname: 'Red' } }";

        component.setDocument(DynamicObject.from(updatedDocumentAsJson));
        component.setFilter(DynamicObject.from(filterAsJson));
        component.initialize();

        Message input = MessageBuilder.get().empty().build();

        // When
        Message actual = component.apply(context, input);

        // Then
        List<Map<String, Object>> results = actual.payload();

        assertExistDocumentWith("{ name: 'Josh', surname: 'Red', age: 55 }");
        assertDocumentsCount(2);
    }
}