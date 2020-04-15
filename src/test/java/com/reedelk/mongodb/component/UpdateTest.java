package com.reedelk.mongodb.component;

import com.reedelk.mongodb.internal.ClientFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    @Test
    void shouldUpdateCorrectly() {
        // Given

        // When

        // Then
    }
}