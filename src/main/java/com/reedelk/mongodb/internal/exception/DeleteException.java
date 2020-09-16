package com.reedelk.mongodb.internal.exception;

import com.reedelk.runtime.api.exception.PlatformException;

public class DeleteException extends PlatformException {

    public DeleteException(String message) {
        super(message);
    }
}
