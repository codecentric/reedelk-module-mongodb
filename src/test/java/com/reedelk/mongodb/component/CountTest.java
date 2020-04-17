package com.reedelk.mongodb.component;

import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.mongodb.internal.exception.MongoDBCountException;
import com.reedelk.runtime.api.commons.ModuleContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.script.dynamicvalue.DynamicObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doAnswer;

class CountTest extends AbstractMongoDBTest {

    private Count component = new Count();

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
    void shouldCorrectlyCountAllDocumentsWithoutFilter() {
        // Given
        insertDocument("{name:'Olav', surname: 'Zipser', age: 55}");
        insertDocument("{name:'Mark', surname: 'Anton', age: 32}");

        component.initialize();

        Message input = MessageBuilder.get(TestComponent.class).empty().build();

        // When
        Message actual = component.apply(context, input);

        // Then
        Long count = actual.payload();
        assertThat(count).isEqualTo(2L);
    }

    @Test
    void shouldCorrectlyCountAllDocumentsWithFilter() {
        // Given
        insertDocument("{name:'Olav', surname: 'Zipser', age: 55}");
        insertDocument("{name:'Mark', surname: 'Anton', age: 32}");
        insertDocument("{name:'Josh', surname: 'Red', age: 49}");

        component.setQuery(DynamicObject.from("{ name: /.*a.*/ }"));
        component.initialize();

        Message input = MessageBuilder.get(TestComponent.class).empty().build();

        // When
        Message actual = component.apply(context, input);

        // Then
        Long count = actual.payload();
        assertThat(count).isEqualTo(2L);
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
        MongoDBCountException thrown =
                assertThrows(MongoDBCountException.class, () -> component.apply(context, input));

        // Then
        assertThat(thrown)
                .hasMessage("The Count filter was null. " +
                        "I cannot execute Count operation with a null filter " +
                        "(DynamicValue=[#[context.myFilter]]).");
    }
}
