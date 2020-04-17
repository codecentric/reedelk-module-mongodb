package com.reedelk.mongodb.internal.commons;

import com.reedelk.runtime.api.exception.PlatformException;
import com.reedelk.runtime.api.message.content.Pair;
import org.bson.Document;

import java.io.Serializable;
import java.util.Map;

public class DocumentUtils {

    public static Document from(Object filter) {
        if (filter instanceof String) {
            return Document.parse((String) filter);
        } else if (filter instanceof Map) {
            // TODO: Check map keys are string
            return new Document((Map) filter);
        } else if (filter instanceof Pair) {
            // TODO: Check pair keys are string todo should I convert it to string using converter?
            Pair<String, Serializable> filterPair = (Pair) filter;
            String key = filterPair.left();
            return new Document(key, filterPair.right());
        } else {
            throw new PlatformException("Type not expected");
        }
    }
}
