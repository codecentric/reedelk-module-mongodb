package com.reedelk.mongodb.component;

import com.reedelk.mongodb.internal.ClientFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

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

}