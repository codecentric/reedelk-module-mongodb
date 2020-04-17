package com.reedelk.mongodb.internal.commons;

import com.reedelk.mongodb.internal.exception.MongoDBFilterException;
import com.reedelk.runtime.api.message.content.DataRow;
import com.reedelk.runtime.api.message.content.DefaultDataRow;
import com.reedelk.runtime.api.message.content.Pair;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.testcontainers.shaded.com.google.common.collect.ImmutableMap.of;

class DocumentUtilsTest {

    @Test
    void shouldCreateDocumentFromDataRow() {
        // Given
        DataRow<Serializable> dataRow =
                new DefaultDataRow(asList("column1", "column2"), asList("One", 2));

        // When
        Document document = DocumentUtils.from(dataRow);

        // Then
        assertThat(document.keySet()).contains("column1", "column2");
        assertThat(document.get("column1")).isEqualTo("One");
        assertThat(document.get("column2")).isEqualTo(2);
        assertThat(document).hasSize(2);
    }

    @Test
    void shouldCreateDocumentFromString() {
        // Given
        String filter = "{'name':'Mark'}";

        // When
        Document document = DocumentUtils.from(filter);

        // Then
        assertThat(document.get("name")).isEqualTo("Mark");
        assertThat(document).hasSize(1);
    }

    @Test
    void shouldCreateDocumentFromMap() {
        // Given
        Map<String, Object> documentMap =
                of("name", "Mark", "age", 43);

        // When
        Document document = DocumentUtils.from(documentMap);

        // Then
        assertThat(document.get("name")).isEqualTo("Mark");
        assertThat(document.get("age")).isEqualTo(43);
        assertThat(document).hasSize(2);
    }

    @Test
    void shouldCreateDocumentFromPair() {
        // Given
        Pair<String, Serializable> pair = Pair.create("name", "Mark");

        // When
        Document document = DocumentUtils.from(pair);

        // Then
        assertThat(document.get("name")).isEqualTo("Mark");
        assertThat(document).hasSize(1);
    }

    @Test
    void shouldThrowNotSupportedTypeWhenInt() {
        // Given
        int wrongTypeFilter = 2;

        // When
        MongoDBFilterException thrown = assertThrows(MongoDBFilterException.class,
                () -> DocumentUtils.from(wrongTypeFilter));

        // Then
        assertThat(thrown).hasMessage("Filter with type=[java.lang.Integer] is not a supported.");
    }
}