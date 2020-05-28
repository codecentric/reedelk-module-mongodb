package com.reedelk.mongodb.internal.attribute;

import com.reedelk.runtime.api.annotation.Type;
import com.reedelk.runtime.api.annotation.TypeProperty;
import com.reedelk.runtime.api.message.MessageAttributes;

import java.util.Optional;

@Type
@TypeProperty(name = CountAttributes.COLLECTION, type = String.class)
@TypeProperty(name = CountAttributes.QUERY, type = String.class)
public class CountAttributes extends MessageAttributes {

    static final String COLLECTION = "collection";
    static final String QUERY = "query";

    public CountAttributes(String collection, Object query) {
        String queryAsString = Optional.ofNullable(query).map(Object::toString).orElse(null);
        put(COLLECTION, collection);
        put(QUERY, queryAsString);
    }
}
