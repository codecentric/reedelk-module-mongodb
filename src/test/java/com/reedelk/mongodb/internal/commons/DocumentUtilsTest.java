package com.reedelk.mongodb.internal.commons;

import com.reedelk.mongodb.internal.exception.MongoDBQueryException;
import com.reedelk.runtime.api.message.content.Pair;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.testcontainers.shaded.com.google.common.collect.ImmutableMap.of;

class DocumentUtilsTest {

    @Test
    void shouldCreateDocumentFromString() {
        // Given
        String filter = "{'name':'Mark'}";

        // When
        Document document = DocumentUtils.from(filter, Unsupported.queryType(filter));

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
        Document document = DocumentUtils.from(documentMap, Unsupported.documentType(documentMap));

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
        Document document = DocumentUtils.from(pair, Unsupported.documentType(pair));

        // Then
        assertThat(document.get("name")).isEqualTo("Mark");
        assertThat(document).hasSize(1);
    }

    @Test
    void shouldThrowNotSupportedTypeWhenInt() {
        // Given
        int wrongTypeFilter = 2;

        // When
        MongoDBQueryException thrown = assertThrows(MongoDBQueryException.class,
                () -> DocumentUtils.from(wrongTypeFilter, Unsupported.queryType(wrongTypeFilter)));

        // Then
        assertThat(thrown).hasMessage("Query with type=[java.lang.Integer] is not a supported.");
    }
}
