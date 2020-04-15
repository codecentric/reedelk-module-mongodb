package com.reedelk.mongodb.internal.exception;

import com.reedelk.runtime.api.exception.PlatformException;

public class MongoDBDeleteException extends PlatformException {

    public MongoDBDeleteException(String message) {
        super(message);
    }
}
