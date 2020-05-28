package com.reedelk.mongodb.internal.commons;

import com.reedelk.mongodb.internal.exception.MongoDBQueryException;
import com.reedelk.runtime.api.converter.ConverterService;
import com.reedelk.runtime.api.message.content.Pair;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.Serializable;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.testcontainers.shaded.com.google.common.collect.ImmutableMap.of;

@ExtendWith(MockitoExtension.class)
class DocumentUtilsTest {

    @Mock
    private ConverterService converterService;

    @Test
    void shouldCreateDocumentFromString() {
        // Given
        String filter = "{'name':'Mark'}";

        // When
        Document document = DocumentUtils.from(converterService, filter, Unsupported.queryType(filter));

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
        Document document = DocumentUtils.from(converterService, documentMap, Unsupported.documentType(documentMap));

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
        Document document = DocumentUtils.from(converterService, pair, Unsupported.documentType(pair));

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
                () -> DocumentUtils.from(converterService, wrongTypeFilter, Unsupported.queryType(wrongTypeFilter)));

        // Then
        assertThat(thrown).hasMessage("Query with type=[java.lang.Integer] is not a supported.");
    }
}
