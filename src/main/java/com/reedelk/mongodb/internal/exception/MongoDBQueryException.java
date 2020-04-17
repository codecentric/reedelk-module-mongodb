package com.reedelk.mongodb.internal.exception;

import com.reedelk.runtime.api.exception.PlatformException;

public class MongoDBQueryException extends PlatformException {

    public MongoDBQueryException(String message) {
        super(message);
    }
}
