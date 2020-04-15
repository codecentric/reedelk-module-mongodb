package com.reedelk.mongodb.component;

import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.mongodb.internal.exception.MongoDBUpdateException;
import com.reedelk.runtime.api.commons.ModuleContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageAttributes;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.script.dynamicvalue.DynamicObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doAnswer;

class DeleteTest extends AbstractMongoDBTest {

    private Delete component = new Delete();

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
        if (component != null) {
            component.dispose();
        }
    }

    @Test
    void shouldCorrectlyDeleteDocumentMatchingFilter() {
        // Given
        insertDocument("{name:'Olav', surname: 'Zipser', age: 55}");
        insertDocument("{name:'Mark', surname: 'Anton', age: 32}");

        String deleteFilter = "{ name: 'Mark' }";
        component.setFilter(DynamicObject.from(deleteFilter));
        component.initialize();

        Message input = MessageBuilder.get().empty().build();

        // When
        Message actual = component.apply(context, input);

        // Then
        MessageAttributes attributes = actual.attributes();
        assertThat(attributes).containsEntry("deleteCount", 1L);
        assertThat(attributes).containsEntry("acknowledge", true);

        assertExistDocumentWith("{name:'Olav', surname: 'Zipser', age: 55}");
        assertDocumentsCount(1);
    }

    @Test
    void shouldCorrectlyDeleteOneWhenMoreThanOneMatching() {
        // Given
        insertDocument("{name:'Olav', surname: 'Zipser', age: 55}");
        insertDocument("{name:'Mark', surname: 'Anton', age: 32}");

        String deleteFilter = "{ age: { $gt: 20 } }";
        component.setFilter(DynamicObject.from(deleteFilter));
        component.initialize();

        Message input = MessageBuilder.get().empty().build();

        // When
        Message actual = component.apply(context, input);

        // Then
        MessageAttributes attributes = actual.attributes();
        assertThat(attributes).containsEntry("deleteCount", 1L);
        assertThat(attributes).containsEntry("acknowledge", true);

        assertDocumentsCount(1);
    }

    @Test
    void shouldCorrectlyDeleteAllWhenMoreThanOneMatching() {
        // Given
        insertDocument("{name:'Olav', surname: 'Zipser', age: 55}");
        insertDocument("{name:'Mark', surname: 'Anton', age: 32}");
        insertDocument("{name:'John', surname: 'Malcom', age: 21}");

        String deleteFilter = "{ age: { $gt: 25 } }";

        component.setFilter(DynamicObject.from(deleteFilter));
        component.setMany(true);
        component.initialize();

        Message input = MessageBuilder.get().empty().build();

        // When
        Message actual = component.apply(context, input);

        // Then
        MessageAttributes attributes = actual.attributes();
        assertThat(attributes).containsEntry("deleteCount", 2L);
        assertThat(attributes).containsEntry("acknowledge", true);

        assertExistDocumentWith("{name:'John', surname: 'Malcom', age: 21}");
        assertDocumentsCount(1);
    }

    @Test
    void shouldThrowExceptionWhenFilterEvaluatesToNull() {
        // Given
        DynamicObject filter = DynamicObject.from("#[context.myFilter]", new ModuleContext(10L));
        component.setFilter(filter);
        component.initialize();

        Message input = MessageBuilder.get().empty().build();

        doAnswer(invocation -> Optional.empty())
                .when(scriptService)
                .evaluate(filter, context, input);

        // When
        MongoDBUpdateException thrown =
                assertThrows(MongoDBUpdateException.class, () -> component.apply(context, input));

        // Then
        assertThat(thrown)
                .hasMessage("The delete document was null. " +
                        "Null documents cannot be updated into MongoDB, " +
                        "did you mean to update with an empty document ({}) ? (DynamicValue=[null]).");

    }
}