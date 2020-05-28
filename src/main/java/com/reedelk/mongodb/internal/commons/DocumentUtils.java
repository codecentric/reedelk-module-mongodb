package com.reedelk.mongodb.internal.commons;

import com.reedelk.mongodb.internal.exception.MongoDBQueryException;
import com.reedelk.runtime.api.converter.ConverterService;
import com.reedelk.runtime.api.exception.PlatformException;
import com.reedelk.runtime.api.message.content.Pair;
import org.bson.Document;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Supplier;

import static com.reedelk.mongodb.internal.commons.Messages.Document.MAP_KEYS_NOT_STRING;
import static com.reedelk.mongodb.internal.commons.Messages.Document.PAIR_LEFT_NOT_STRING;

public class DocumentUtils {

    private DocumentUtils() {
    }

    @SuppressWarnings("unchecked")
    public static Document from(ConverterService converterService, Object documentObject, Supplier<? extends PlatformException> exception) {

        // Could be a byte array containing the JSON sent with a post to a REST Listener.
        if (documentObject instanceof byte[] || documentObject instanceof Byte[]) {
            String converted = converterService.convert(documentObject, String.class);
            return Document.parse(converted);
        }
        // If documentObject instanceof byte[] -> converter.convert(to string)?
        if (documentObject instanceof String) {
            return Document.parse((String) documentObject);

        } else if (documentObject instanceof Map) {
            checkKeysAreStringTypeOrThrow((Map<Object, Object>) documentObject);
            return new Document((Map<String, Object>) documentObject);

        } else if (documentObject instanceof Pair) {
            Pair<Serializable, Serializable> queryPair = (Pair<Serializable, Serializable>) documentObject;
            checkLeftIsStringTypeOrThrow(queryPair);
            String key = (String) queryPair.left();
            return new Document(key, queryPair.right());

        } else {
            throw exception.get();
        }
    }

    private static void checkLeftIsStringTypeOrThrow(Pair<Serializable,Serializable> pair) {
        if (!(pair.left() instanceof String)) {
            String error = PAIR_LEFT_NOT_STRING.format(Utils.classNameOrNull(pair.left()));
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
