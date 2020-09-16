package com.reedelk.mongodb.internal.exception;

import com.reedelk.runtime.api.exception.PlatformException;

public class QueryException extends PlatformException {

    public QueryException(String message) {
        super(message);
    }
}
