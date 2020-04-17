package com.reedelk.mongodb.component;

import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.mongodb.internal.exception.MongoDBDeleteException;
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
        super.tearDown();
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
        component.setQuery(DynamicObject.from(deleteFilter));
        component.initialize();

        Message input = MessageBuilder.get(TestComponent.class).empty().build();

        // When
        Message actual = component.apply(context, input);

        // Then
        long deleteCount = actual.payload();
        assertThat(deleteCount).isEqualTo(1L);

        MessageAttributes attributes = actual.attributes();
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
        component.setQuery(DynamicObject.from(deleteFilter));
        component.initialize();

        Message input = MessageBuilder.get(TestComponent.class).empty().build();

        // When
        Message actual = component.apply(context, input);

        // Then
        long deleteCount = actual.payload();
        assertThat(deleteCount).isEqualTo(1L);

        MessageAttributes attributes = actual.attributes();
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

        component.setQuery(DynamicObject.from(deleteFilter));
        component.setMany(true);
        component.initialize();

        Message input = MessageBuilder.get(TestComponent.class).empty().build();

        // When
        Message actual = component.apply(context, input);

        // Then
        long deleteCount = actual.payload();
        assertThat(deleteCount).isEqualTo(2L);

        MessageAttributes attributes = actual.attributes();
        assertThat(attributes).containsEntry("acknowledge", true);

        assertExistDocumentWith("{name:'John', surname: 'Malcom', age: 21}");
        assertDocumentsCount(1);
    }

    @Test
    void shouldThrowExceptionWhenFilterEvaluatesToNull() {
        // Given
        DynamicObject filter = DynamicObject.from("#[context.myFilter]", new ModuleContext(10L));
        component.setQuery(filter);
        component.initialize();

        Message input = MessageBuilder.get(TestComponent.class).empty().build();

        doAnswer(invocation -> Optional.empty())
                .when(scriptService)
                .evaluate(filter, context, input);

        // When
        MongoDBDeleteException thrown =
                assertThrows(MongoDBDeleteException.class, () -> component.apply(context, input));

        // Then
        assertThat(thrown)
                .hasMessage("The Delete filter was null. " +
                        "I cannot execute Delete operation with a null filter " +
                        "(DynamicValue=[#[context.myFilter]]).");
    }
}
