package com.reedelk.mongodb.internal.attribute;

import com.reedelk.runtime.api.annotation.Type;
import com.reedelk.runtime.api.annotation.TypeProperty;
import com.reedelk.runtime.api.message.MessageAttributes;

import java.util.Optional;

import static com.reedelk.mongodb.internal.attribute.DeleteAttributes.*;

@Type
@TypeProperty(name = QUERY, type = String.class)
@TypeProperty(name = DELETED_COUNT, type = long.class)
@TypeProperty(name = ACKNOWLEDGE, type = boolean.class)
public class DeleteAttributes extends MessageAttributes {

    static final String QUERY = "query";
    static final String ACKNOWLEDGE = "acknowledge";
    static final String DELETED_COUNT = "deletedCount";

    public DeleteAttributes(long deletedCount, boolean acknowledged, Object query) {
        String queryAsString = Optional.ofNullable(query).map(Object::toString).orElse(null);
        put(QUERY, queryAsString);
        put(ACKNOWLEDGE, acknowledged);
        put(DELETED_COUNT, deletedCount);
    }
}
