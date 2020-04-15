package com.reedelk.mongodb.internal.exception;

import com.reedelk.runtime.api.exception.PlatformException;

public class MongoDBInsertException extends PlatformException {

    public MongoDBInsertException(String message) {
        super(message);
    }
}
