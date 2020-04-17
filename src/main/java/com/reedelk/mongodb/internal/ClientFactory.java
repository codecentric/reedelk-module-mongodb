package com.reedelk.mongodb.internal;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.reedelk.mongodb.component.ConnectionConfiguration;
import com.reedelk.runtime.api.commons.StringUtils;
import org.osgi.service.component.annotations.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotBlank;
import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotNull;

@Component(service = ClientFactory.class)
public class ClientFactory {

    private final Map<String, ConnectionHolder> configIdClientMap = new HashMap<>();

    public synchronized MongoClient clientByConfig(com.reedelk.runtime.api.component.Component component,
                                                   ConnectionConfiguration connection) {

        requireNotNull(component.getClass(), connection, "MongoDB connection must not be null");

        String database = connection.getDatabase();
        String connectionURL = connection.getConnectionURL();
        requireNotBlank(component.getClass(), database, "MongoDB database must not be null");
        requireNotBlank(component.getClass(), connectionURL, "MongoDB connection url must not be empty");

        String connectionId = connection.getId();

        if (!configIdClientMap.containsKey(connectionId)) {

            MongoClientSettings.Builder builder = MongoClientSettings.builder();

            String username = connection.getUsername();
            String password = connection.getPassword();
            if (StringUtils.isNotBlank(username)) {
                MongoCredential credential = MongoCredential.createCredential(username, database, password.toCharArray());
                builder.credential(credential);
            }

            MongoClientSettings settings = builder
                    .applyConnectionString(new ConnectionString(connectionURL))
                    .build();

            MongoClient client = MongoClients.create(settings);
            ConnectionHolder connectionHolder = new ConnectionHolder(client);
            configIdClientMap.put(connectionId, connectionHolder);
        }

        ConnectionHolder connectionHolder = configIdClientMap.get(connectionId);
        connectionHolder.components.add(component);
        return connectionHolder.client;
    }

    public synchronized void dispose(
            com.reedelk.runtime.api.component.Component component,
            ConnectionConfiguration connection) {

        String connectionId = connection.getId();

        if (configIdClientMap.containsKey(connectionId)) {
            ConnectionHolder connectionHolder = configIdClientMap.get(connectionId);
            connectionHolder.components.remove(component);
            if (connectionHolder.components.isEmpty()) {
                configIdClientMap.remove(connectionId); // There are no clients using this config.
                connectionHolder.client.close();
            }
        }
    }

    public synchronized void dispose() {
        configIdClientMap.values()
                .forEach(connectionHolder -> connectionHolder.client.close());
    }

    static class ConnectionHolder {

        final MongoClient client;
        final List<com.reedelk.runtime.api.component.Component> components = new ArrayList<>();

        public ConnectionHolder(MongoClient client) {
            this.client = client;
        }
    }
}
