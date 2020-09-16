package com.reedelk.mongodb.internal.commons;

import com.reedelk.mongodb.internal.exception.DocumentException;
import com.reedelk.mongodb.internal.exception.QueryException;

import java.util.function.Supplier;

import static com.reedelk.mongodb.internal.commons.Messages.Document.DOCUMENT_NOT_SUPPORTED;
import static com.reedelk.mongodb.internal.commons.Messages.Document.QUERY_TYPE_NOT_SUPPORTED;

public class Unsupported {

    private Unsupported() {
    }

    public static Supplier<QueryException> queryType(Object query){
        return () -> {
            String error = QUERY_TYPE_NOT_SUPPORTED.format(Utils.classNameOrNull(query));
            return new QueryException(error);
        };
    }

    public static Supplier<DocumentException> documentType(Object document){
        return () -> {
            String error = DOCUMENT_NOT_SUPPORTED.format(Utils.classNameOrNull(document));
            return new DocumentException(error);
        };
    }
}
