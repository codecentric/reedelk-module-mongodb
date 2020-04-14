package com.reedelk.mongodb.component;

import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.component.Implementor;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ServiceScope;

@Shared
@Component(service = MongoDBConnection.class, scope = ServiceScope.PROTOTYPE)
public class MongoDBConnection implements Implementor {

    @Property("Connection URL")
    @Example("mongodb://localhost:27017")
    @Hint("mongodb://localhost:27017")
    @InitValue("mongodb://localhost:27017")
    @Description("The connection URL is a string that a MongoDB driver uses to connect to a database. " +
            "It can contain information such as where to search for the database, " +
            "the name of the database to connect to, and configuration properties.")
    private String connectionURL;

    @Property("Database")
    @Hint("myDatabase")
    @Example("myOrders")
    @Description("The Database we want to connect to")
    private String database;

    public String getConnectionURL() {
        return connectionURL;
    }

    public void setConnectionURL(String connectionURL) {
        this.connectionURL = connectionURL;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }
}
