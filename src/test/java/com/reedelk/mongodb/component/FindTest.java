package com.reedelk.mongodb.component;

import com.reedelk.mongodb.internal.ClientFactory;
import com.reedelk.mongodb.internal.exception.MongoDBFindException;
import com.reedelk.runtime.api.commons.ModuleContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.message.content.Pair;
import com.reedelk.runtime.api.script.dynamicvalue.DynamicObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doAnswer;
import static org.testcontainers.shaded.com.google.common.collect.ImmutableMap.of;

class FindTest extends AbstractMongoDBTest {

    private Find component = new Find();

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

    @AfterEach
    void tearDown() {
        if (component != null) {
            component.dispose();
        }
    }

    @Test
    void shouldCorrectlyFindItemsWithoutFilter() {
        // Given
        component.initialize();

        Message message = MessageBuilder.get().empty().build();

        // When
        Message actual = component.apply(context, message);

        // Then
        List<Map<String, Object>> results = actual.payload();

        assertExistEntry(results, of("name", "Olav", "surname", "Zipser", "age", 55));
        assertExistEntry(results, of("name", "Mark", "surname", "Anton", "age", 32));
    }

    // Filter is JSON string
    @Test
    void shouldCorrectlyFindItemsWithStringJSONFilter() {
        // Given
        String filterAsString = "{ 'name': 'Olav' }";
        DynamicObject filter = DynamicObject.from(filterAsString);
        component.setFilter(filter);
        component.initialize();

        Message input = MessageBuilder.get().empty().build();

        // When
        Message actual = component.apply(context, input);

        // Then
        List<Map<String,Object>> results = actual.payload();

        assertThat(results).hasSize(1);
        assertExistEntry(results, of("name", "Olav", "surname", "Zipser", "age", 55));
    }

    // Filter is a Map
    @Test
    void shouldCorrectlyFindItemsWithMapFilter() {
        // Given
        Map<String, Object> filterAsMap = ImmutableMap.of("name", "Olav");
        DynamicObject filter = DynamicObject.from(filterAsMap);
        component.setFilter(filter);
        component.initialize();

        Message input = MessageBuilder.get().empty().build();

        // When
        Message actual = component.apply(context, input);

        // Then
        List<Map<String,Object>> results = actual.payload();

        assertThat(results).hasSize(1);
        assertExistEntry(results, of("name", "Olav", "surname", "Zipser", "age", 55));
    }

    // Filter is a Pair
    @Test
    void shouldCorrectlyFindItemsWithPair() {
        // Given
        Pair<String, Integer> filterAsPair = Pair.create("age", 32);
        DynamicObject filter = DynamicObject.from(filterAsPair);
        component.setFilter(filter);
        component.initialize();

        Message input = MessageBuilder.get().empty().build();

        // When
        Message actual = component.apply(context, input);

        // Then
        List<Map<String,Object>> results = actual.payload();

        assertThat(results).hasSize(1);
        assertExistEntry(results, of("name", "Mark", "surname", "Anton", "age", 32));
    }

    @Test
    void shouldReturnEmptyResultsWithFilter() {
        // Given
        DynamicObject filter = DynamicObject.from("{ 'name': 'NotExistent' }");
        component.setFilter(filter);
        component.initialize();

        Message input = MessageBuilder.get().empty().build();

        // When
        Message actual = component.apply(context, input);

        // Then
        List<Map<String,Object>> results = actual.payload();
        assertThat(results).isEmpty();
    }

    @Test
    void shouldThrowExceptionWhenFindFilterEvaluatesToNull() {
        // Given
        DynamicObject filter = DynamicObject.from("#[context.myFilter]", new ModuleContext(10L));
        component.setFilter(filter);
        component.initialize();

        Message input = MessageBuilder.get().empty().build();

        doAnswer(invocation -> Optional.empty())
                .when(scriptService)
                .evaluate(filter, context, input);

        // When
        MongoDBFindException thrown =
                assertThrows(MongoDBFindException.class, () -> component.apply(context, input));

        // Then
        assertThat(thrown).hasMessage("The Find filter was null. I cannot execute find operation with a null filter (DynamicValue=[#[context.myFilter]]).");
    }
}