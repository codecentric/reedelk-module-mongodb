package com.reedelk.mongodb.internal.attribute;

import com.reedelk.runtime.api.annotation.Type;
import com.reedelk.runtime.api.annotation.TypeProperty;
import com.reedelk.runtime.api.message.MessageAttributes;

import static com.reedelk.mongodb.internal.attribute.DeleteAttributes.ACKNOWLEDGE;
import static com.reedelk.mongodb.internal.attribute.DeleteAttributes.DELETED_COUNT;

@Type
@TypeProperty(name = ACKNOWLEDGE, type = boolean.class)
@TypeProperty(name = DELETED_COUNT, type = long.class)
public class DeleteAttributes extends MessageAttributes {

    static final String ACKNOWLEDGE = "acknowledge";
    static final String DELETED_COUNT = "deletedCount";

    public DeleteAttributes(long deletedCount, boolean acknowledged) {
        put(ACKNOWLEDGE, acknowledged);
        put(DELETED_COUNT, deletedCount);
    }
}
