package com.reedelk.mongodb.component;

import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.mongodb.internal.exception.MongoDBUpdateException;
import com.reedelk.runtime.api.commons.ModuleContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageAttributes;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.script.dynamicvalue.DynamicObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doAnswer;

class UpdateTest extends AbstractMongoDBTest {

    private Update component = new Update();

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
        super.tearDown();
        if (component != null) {
            component.dispose();
        }
    }

    @Test
    void shouldCorrectlyUpdateDocument() {
        // Given
        insertDocument("{name:'Olav', surname: 'Zipser', age: 55}");
        insertDocument("{name:'Mark', surname: 'Anton', age: 32}");

        String filterAsJson = "{ name: 'Olav' }";
        String updatedDocumentAsJson = "{\"$set\": {\"name\": \"Josh\", \"surname\": \"Red\"}}";

        component.setDocument(DynamicObject.from(updatedDocumentAsJson));
        component.setFilter(DynamicObject.from(filterAsJson));
        component.initialize();

        Message input = MessageBuilder.get(TestComponent.class).empty().build();

        // When
        Message actual = component.apply(context, input);

        // Then
        Long modifiedCount = actual.payload();

        // The payload contains the modified count.
        assertThat(modifiedCount).isEqualTo(1);

        MessageAttributes attributes = actual.attributes();

        assertThat(attributes).containsEntry("matchedCount", 1L);
        assertThat(attributes).containsEntry("component", "com.reedelk.mongodb.component.Update");

        assertExistDocumentWith("{ name: 'Josh', surname: 'Red', age: 55 }");
        assertExistDocumentWith("{ name:'Mark', surname: 'Anton', age: 32}");
        assertDocumentsCount(2);
    }

    @Test
    void shouldThrowExceptionWhenDocumentEvaluatesToNull() {
        // Given
        String filterAsJson = "{ name: 'Olav' }";
        DynamicObject document = DynamicObject.from(null);

        component.setFilter(DynamicObject.from(filterAsJson));
        component.setDocument(document);
        component.initialize();

        Message input = MessageBuilder.get(TestComponent.class).empty().build();

        doAnswer(invocation -> Optional.empty())
                .when(scriptService)
                .evaluate(document, context, input);

        // When
        MongoDBUpdateException thrown =
                assertThrows(MongoDBUpdateException.class, () -> component.apply(context, input));

        // Then
        assertThat(thrown)
                .hasMessage("The updated document was null. " +
                        "Null documents cannot be updated into MongoDB, " +
                        "did you mean to update with an empty document ({}) ? (DynamicValue=[null]).");
    }

    @Test
    void shouldThrowExceptionWhenFilterEvaluatesToNull() {
        // Given
        DynamicObject filter = DynamicObject.from("#[context.myFilter]", new ModuleContext(10L));

        String updatedDocumentAsJson = "{\"$set\": {\"name\": \"Josh\", \"surname\": \"Red\"}}";
        DynamicObject document = DynamicObject.from(updatedDocumentAsJson);

        component.setDocument(document);
        component.setFilter(filter);
        component.initialize();

        Message input = MessageBuilder.get(TestComponent.class).empty().build();

        doAnswer(invocation -> Optional.empty())
                .when(scriptService)
                .evaluate(filter, context, input);

        // When
        MongoDBUpdateException thrown =
                assertThrows(MongoDBUpdateException.class, () -> component.apply(context, input));

        // Then
        assertThat(thrown)
                .hasMessage("The Update filter was null. " +
                        "I cannot execute Update operation with a null filter " +
                        "(DynamicValue=[#[context.myFilter]]).");
    }

    @Test
    void shouldUpdateManyDocuments() {
        // Given
        insertDocument("{name:'Olav', surname: 'Zipser', age: 55}");
        insertDocument("{name:'Mark', surname: 'Anton', age: 32}");
        insertDocument("{name:'John', surname: 'Red', age: 45}");

        String filterAsJson = "{ age: { $gt: 40 } }";
        String updatedDocumentAsJson = "{\"$set\": {\"olderThan\": true }}";

        component.setDocument(DynamicObject.from(updatedDocumentAsJson));
        component.setFilter(DynamicObject.from(filterAsJson));
        component.setMany(true);
        component.initialize();

        Message input = MessageBuilder.get(TestComponent.class).empty().build();

        // When
        Message actual = component.apply(context, input);

        // Then
        Long payload = actual.payload();

        // The payload contains the modified count.
        assertThat(payload).isEqualTo(2L);

        MessageAttributes attributes = actual.attributes();

        assertThat(attributes).containsEntry("matchedCount", 2L);
        assertThat(attributes).containsEntry("component", "com.reedelk.mongodb.component.Update");

        assertExistDocumentWith("{ name: 'Olav', surname: 'Zipser', age: 55, olderThan: true }");
        assertExistDocumentWith("{ name:'John', surname: 'Red', age: 45, olderThan: true }");
        assertExistDocumentWith("{ name:'Mark', surname: 'Anton', age: 32}");
        assertDocumentsCount(3);}
}
