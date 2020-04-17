package com.reedelk.mongodb.internal.commons;

import com.reedelk.mongodb.internal.exception.MongoDBFilterException;
import com.reedelk.runtime.api.message.content.Pair;
import org.bson.Document;

import java.io.Serializable;
import java.util.Map;

import static com.reedelk.mongodb.internal.commons.Messages.Document.*;

public class DocumentUtils {

    @SuppressWarnings("unchecked")
    public static Document from(Object filter) {

        if (filter instanceof String) {
            return Document.parse((String) filter);

        } else if (filter instanceof Map) {
            checkKeysAreStringTypeOrThrow((Map<Object, Object>) filter);
            return new Document((Map<String, Object>) filter);

        } else if (filter instanceof Pair) {
            Pair<Serializable, Serializable> filterPair = (Pair<Serializable, Serializable>) filter;
            checkLeftIsStringTypeOrThrow(filterPair);
            String key = (String) filterPair.left();
            return new Document(key, filterPair.right());

        } else {
            String error = FILTER_NOT_SUPPORTED.format(Utils.getClassOrNull(filter));
            throw new MongoDBFilterException(error);
        }
    }

    private static void checkLeftIsStringTypeOrThrow(Pair<Serializable,Serializable> pair) {
        if (!(pair.left() instanceof String)) {
            String error = PAIR_LEFT_NOT_STRING.format(Utils.getClassOrNull(pair.left()));
            throw new MongoDBFilterException(error);
        }
    }

    private static void checkKeysAreStringTypeOrThrow(Map<Object, Object> filter) {
        boolean areAllStrings = filter.keySet().stream().allMatch(key -> key instanceof String);
        if (!areAllStrings) {
            String error = MAP_KEYS_NOT_STRING.format();
            throw new MongoDBFilterException(error);
        }
    }
}
