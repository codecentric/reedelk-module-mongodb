package com.reedelk.mongodb.internal.commons;

import com.reedelk.mongodb.internal.exception.MongoDBDocumentException;
import com.reedelk.mongodb.internal.exception.MongoDBQueryException;

import java.util.function.Supplier;

import static com.reedelk.mongodb.internal.commons.Messages.Document.DOCUMENT_NOT_SUPPORTED;
import static com.reedelk.mongodb.internal.commons.Messages.Document.QUERY_TYPE_NOT_SUPPORTED;

public class Unsupported {

    private Unsupported() {
    }

    public static Supplier<MongoDBQueryException> queryType(Object query){
        return () -> {
            String error = QUERY_TYPE_NOT_SUPPORTED.format(Utils.classNameOrNull(query));
            return new MongoDBQueryException(error);
        };
    }

    public static Supplier<MongoDBDocumentException> documentType(Object document){
        return () -> {
            String error = DOCUMENT_NOT_SUPPORTED.format(Utils.classNameOrNull(document));
            return new MongoDBDocumentException(error);
        };
    }
}
