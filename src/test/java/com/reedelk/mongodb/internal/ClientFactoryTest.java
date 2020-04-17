package com.reedelk.mongodb.internal;

import com.mongodb.client.MongoClient;
import com.reedelk.mongodb.component.ConnectionConfiguration;
import com.reedelk.mongodb.component.Insert;
import com.reedelk.mongodb.component.Update;
import com.reedelk.runtime.api.component.Component;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ClientFactoryTest {

    @Mock
    private MongoClient client1;
    @Mock
    private MongoClient client2;

    private ClientFactory factory = spy(new ClientFactory());

    private Insert insert = new Insert();
    private Update update = new Update();

    @BeforeEach
    void setUp() {
        doReturn(client1, client2)
                .when(factory)
                .createClient(any(ConnectionConfiguration.class));
    }

    @Test
    void shouldCreateNewConfigurationIfDoesNotExists() {
        // Given
        String configId = UUID.randomUUID().toString();
        ConnectionConfiguration configuration = createConfiguration(configId);

        // When
        MongoClient actual = factory.clientByConfig(insert, configuration);

        // Then
        assertThat(actual).isNotNull();
        assertThat(factory.configIdClientMap).containsKey(configId);
        verify(factory).createClient(configuration);
    }

    @Test
    void shouldReturnSameConfigurationIfExistsAlready() {
        // Given
        String configId = UUID.randomUUID().toString();
        ConnectionConfiguration configuration = createConfiguration(configId);
        MongoClient insertClient = factory.clientByConfig(insert, configuration);

        // When
        MongoClient updateClient = factory.clientByConfig(update, configuration);

        // Then
        assertThat(updateClient).isEqualTo(insertClient);
        assertThat(factory.configIdClientMap).containsKey(configId);
        verify(factory).createClient(configuration);

        ClientFactory.ConnectionHolder connectionHolder = factory.configIdClientMap.get(configId);
        List<Component> componentsByClient = connectionHolder.components;
        assertThat(componentsByClient).hasSize(2);
        assertThat(componentsByClient).containsExactly(insert, update);
    }

    @Test
    void shouldReleaseConfigurationCorrectly() {
        // Given
        String configId = UUID.randomUUID().toString();
        ConnectionConfiguration configuration = createConfiguration(configId);
        factory.clientByConfig(insert, configuration);
        factory.clientByConfig(update, configuration);

        // When
        factory.dispose(update, configuration);

        // Then
        assertThat(factory.configIdClientMap).containsKey(configId); // Because Insert is still using it.

        verify(client1, never()).close();

        ClientFactory.ConnectionHolder connectionHolder = factory.configIdClientMap.get(configId);
        List<Component> componentsByClient = connectionHolder.components;
        assertThat(componentsByClient).hasSize(1);
        assertThat(componentsByClient).containsExactly(insert); // Update component has been removed.
    }

    @Test
    void shouldReleaseConfigurationCorrectlyAndRemoveWhenNoConsumers() {
        // Given
        String configId = UUID.randomUUID().toString();
        ConnectionConfiguration configuration = createConfiguration(configId);
        factory.clientByConfig(insert, configuration);
        factory.clientByConfig(update, configuration);

        // When
        factory.dispose(update, configuration);
        factory.dispose(insert, configuration);

        // Then
        assertThat(factory.configIdClientMap).isEmpty(); // Because Insert and Update disposed it.
        verify(client1).close();
    }

    @Test
    void shouldDisposeAll() {
        // Given
        String configId1 = UUID.randomUUID().toString();
        String configId2 = UUID.randomUUID().toString();
        ConnectionConfiguration configuration1 = createConfiguration(configId1);
        ConnectionConfiguration configuration2 = createConfiguration(configId2);
        factory.clientByConfig(insert, configuration1);
        factory.clientByConfig(update, configuration2);

        // When
        factory.dispose();

        // Then
        assertThat(factory.configIdClientMap).isEmpty(); // Because Insert and Update disposed it.
        verify(client1).close();
        verify(client2).close();
    }

    @Test
    void shouldDisposeOnlyOneConnection() {
        // Given
        String configId1 = UUID.randomUUID().toString();
        String configId2 = UUID.randomUUID().toString();
        ConnectionConfiguration configuration1 = createConfiguration(configId1);
        ConnectionConfiguration configuration2 = createConfiguration(configId2);
        factory.clientByConfig(insert, configuration1);
        factory.clientByConfig(update, configuration2);

        // When
        factory.dispose(insert, configuration1);

        // Then
        assertThat(factory.configIdClientMap).containsOnlyKeys(configId2);

        ClientFactory.ConnectionHolder connectionHolder = factory.configIdClientMap.get(configId2);
        List<Component> components = connectionHolder.components;
        assertThat(components).containsExactly(update);

        verify(client1).close();
        verify(client2, never()).close();
    }

    private ConnectionConfiguration createConfiguration(String configId) {
        ConnectionConfiguration configuration = new ConnectionConfiguration();
        configuration.setConnectionURL("mongodb://localhost:27017");
        configuration.setDatabase("myDatabase");
        configuration.setUsername("myUsername");
        configuration.setPassword("myPassword");
        configuration.setId(configId);
        return configuration;
    }

}
