package com.reedelk.mongodb.component;

import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.component.Implementor;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ServiceScope;

@Shared
@Component(service = MongoDBConnection.class, scope = ServiceScope.PROTOTYPE)
public class MongoDBConnection implements Implementor {

    @Property("Host")
    @Hint("mongodb.server.domain.com")
    @Example("mongodb.server.domain.com")
    @InitValue("localhost")
    @DefaultValue("localhost")
    @Description("The MongoDB host to be used for the connection.")
    private String host;

    @Property("Port")
    @Hint("27017")
    @Example("27017")
    @Description("The MongoDB port to be used for the connection.")
    private Integer port;

    @Property("Database")
    @Hint("myDatabase")
    @Example("myOrders")
    @Description("The Database we want to connect to")
    private String database;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getDatabase() {
        return database;
    }
}
