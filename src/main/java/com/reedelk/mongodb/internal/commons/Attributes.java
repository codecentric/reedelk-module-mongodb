package com.reedelk.mongodb.internal.commons;

import com.mongodb.client.result.UpdateResult;
import com.reedelk.runtime.api.commons.ImmutableMap;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

public class Attributes {

    public static Map<String, Serializable> from(UpdateResult updateResult) {
        long matchedCount = updateResult.getMatchedCount();
        long modifiedCount = updateResult.getModifiedCount();
        String upsertedId = Optional.ofNullable(updateResult.getUpsertedId())
                .map(Object::toString)
                .orElse(null);

        return ImmutableMap.of(
                "matchedCount", matchedCount,
                "modifiedCount", modifiedCount,
                "upsertedId", upsertedId);
    }
}
