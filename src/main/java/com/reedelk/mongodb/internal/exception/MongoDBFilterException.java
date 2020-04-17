package com.reedelk.mongodb.internal.exception;

import com.reedelk.runtime.api.exception.PlatformException;

public class MongoDBFilterException extends PlatformException {

    public MongoDBFilterException(String message) {
        super(message);
    }
}
