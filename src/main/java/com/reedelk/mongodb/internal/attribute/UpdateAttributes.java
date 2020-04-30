package com.reedelk.mongodb.internal.attribute;

import com.mongodb.client.result.UpdateResult;
import com.reedelk.runtime.api.annotation.Type;
import com.reedelk.runtime.api.annotation.TypeProperty;
import com.reedelk.runtime.api.message.MessageAttributes;

import java.util.Optional;

import static com.reedelk.mongodb.internal.attribute.UpdateAttributes.MATCHED_COUNT;
import static com.reedelk.mongodb.internal.attribute.UpdateAttributes.UPSERTED_ID;

@Type
@TypeProperty(name = MATCHED_COUNT, type = long.class)
@TypeProperty(name = UPSERTED_ID, type = String.class)
public class UpdateAttributes extends MessageAttributes {

    static final String MATCHED_COUNT = "matchedCount";
    static final String UPSERTED_ID = "upsertedId";

    public UpdateAttributes(UpdateResult updateResult) {
        long matchedCount = updateResult.getMatchedCount();
        String upsertedId = Optional.ofNullable(updateResult.getUpsertedId())
                .map(Object::toString)
                .orElse(null);
        put(MATCHED_COUNT, matchedCount);
        put(UPSERTED_ID, upsertedId);
    }
}
