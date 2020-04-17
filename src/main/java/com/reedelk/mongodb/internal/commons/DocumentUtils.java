package com.reedelk.mongodb.internal.commons;

import com.reedelk.mongodb.internal.exception.MongoDBQueryException;
import com.reedelk.runtime.api.exception.PlatformException;
import com.reedelk.runtime.api.message.content.DataRow;
import com.reedelk.runtime.api.message.content.Pair;
import org.bson.Document;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static com.reedelk.mongodb.internal.commons.Messages.Document.MAP_KEYS_NOT_STRING;
import static com.reedelk.mongodb.internal.commons.Messages.Document.PAIR_LEFT_NOT_STRING;

public class DocumentUtils {

    private DocumentUtils() {
    }

    @SuppressWarnings("unchecked")
    public static Document from(Object query, Supplier<? extends PlatformException> exception) {

        if (query instanceof String) {
            return Document.parse((String) query);

        } else if (query instanceof Map) {
            checkKeysAreStringTypeOrThrow((Map<Object, Object>) query);
            return new Document((Map<String, Object>) query);

        } else if (query instanceof Pair) {
            Pair<Serializable, Serializable> queryPair = (Pair<Serializable, Serializable>) query;
            checkLeftIsStringTypeOrThrow(queryPair);
            String key = (String) queryPair.left();
            return new Document(key, queryPair.right());

        } else if (query instanceof DataRow) {
            DataRow<Serializable> dataRow = (DataRow<Serializable>) query;
            Map<String, Object> dataAsMap = new HashMap<>(dataRow.asMap());
            return new Document(dataAsMap);

        } else {
            throw exception.get();
        }
    }

    private static void checkLeftIsStringTypeOrThrow(Pair<Serializable,Serializable> pair) {
        if (!(pair.left() instanceof String)) {
            String error = PAIR_LEFT_NOT_STRING.format(Utils.getClassOrNull(pair.left()));
            throw new MongoDBQueryException(error);
        }
    }

    private static void checkKeysAreStringTypeOrThrow(Map<Object, Object> query) {
        boolean areAllStrings = query.keySet().stream().allMatch(key -> key instanceof String);
        if (!areAllStrings) {
            String error = MAP_KEYS_NOT_STRING.format();
            throw new MongoDBQueryException(error);
        }
    }
}
