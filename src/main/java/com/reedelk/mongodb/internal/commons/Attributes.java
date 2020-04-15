package com.reedelk.mongodb.internal.commons;

import com.mongodb.client.result.UpdateResult;
import com.reedelk.mongodb.component.Update;
import com.reedelk.runtime.api.commons.ImmutableMap;
import com.reedelk.runtime.api.message.DefaultMessageAttributes;
import com.reedelk.runtime.api.message.MessageAttributes;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

public class Attributes {

    public static MessageAttributes from(UpdateResult updateResult) {
        long matchedCount = updateResult.getMatchedCount();
        long modifiedCount = updateResult.getModifiedCount();
        String upsertedId = Optional.ofNullable(updateResult.getUpsertedId())
                .map(Object::toString)
                .orElse(null);

        Map<String, Serializable> componentAttributes = ImmutableMap.of(
                "matchedCount", matchedCount,
                "modifiedCount", modifiedCount,
                "upsertedId", upsertedId);

        return new DefaultMessageAttributes(Update.class, componentAttributes);
    }
}
