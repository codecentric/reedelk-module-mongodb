package com.reedelk.mongodb.internal.exception;

import com.reedelk.runtime.api.exception.PlatformException;

public class MongoDBCountException extends PlatformException {

    public MongoDBCountException(String message) {
        super(message);
    }
}
