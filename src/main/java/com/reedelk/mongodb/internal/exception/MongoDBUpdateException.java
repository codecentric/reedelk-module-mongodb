package com.reedelk.mongodb.internal.exception;

import com.reedelk.runtime.api.exception.PlatformException;

public class MongoDBUpdateException extends PlatformException {

    public MongoDBUpdateException(String message) {
        super(message);
    }
}
