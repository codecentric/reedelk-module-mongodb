package com.reedelk.mongodb.internal.attribute;

import com.reedelk.runtime.api.annotation.Type;
import com.reedelk.runtime.api.annotation.TypeProperty;
import com.reedelk.runtime.api.message.MessageAttributes;

import java.util.Optional;

import static com.reedelk.mongodb.internal.attribute.FindAttributes.COLLECTION;
import static com.reedelk.mongodb.internal.attribute.FindAttributes.QUERY;

@Type
@TypeProperty(name = COLLECTION, type = String.class)
@TypeProperty(name = QUERY, type = String.class)
public class FindAttributes extends MessageAttributes {

    static final String COLLECTION = "collection";
    static final String QUERY = "query";

    public FindAttributes(String collection, Object query) {
        String queryAsString = Optional.ofNullable(query).map(Object::toString).orElse(null);
        put(COLLECTION, collection);
        put(QUERY, queryAsString);
    }
}
