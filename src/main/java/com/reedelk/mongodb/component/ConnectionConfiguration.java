package com.reedelk.mongodb.component;

import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.component.Implementor;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ServiceScope;

@Shared
@Component(service = ConnectionConfiguration.class, scope = ServiceScope.PROTOTYPE)
public class ConnectionConfiguration implements Implementor {

    @Property("id")
    @Hidden
    private String id;

    @Property("Connection URL")
    @Example("mongodb://localhost:27017")
    @Hint("mongodb://localhost:27017")
    @InitValue("mongodb://localhost:27017")
    @Description("The connection URL is a string that a MongoDB driver uses to connect to a database. " +
            "It can contain information such as where to search for the database, " +
            "the name of the database to connect to, and configuration properties.")
    private String connectionURL;

    @Property("Username")
    @Hint("myMongoDBUser")
    @Example("myMongoDBUser")
    @Description("The username to be used to create the MongoDB connection.")
    private String username;

    @Property("Password")
    @Password
    @Example("myMongoDBPassword")
    @Description("The password to be used to create the MongoDB connection.")
    private String password;

    @Property("Database Name")
    @Hint("myDatabase")
    @Example("myOrdersDatabase")
    @Description("The database name we want to connect to")
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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
