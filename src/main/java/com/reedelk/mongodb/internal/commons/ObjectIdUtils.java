package com.reedelk.mongodb.internal.commons;

import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.Set;

import static com.reedelk.runtime.api.commons.ImmutableMap.of;

public class ObjectIdUtils {

    public static final String OBJECT_ID_PROPERTY = "_id";

    // We don't want to return (ObjectId) object.
    public static Object replace(Object id) {
        if (id instanceof ObjectId) {
            ObjectId theId = (ObjectId) id;
            return theId.toHexString();
        } else {
            return id;
        }
    }

    // Convert Extended Object id into a hex ID.
    // This only if the ID has type Object ID, otherwise it means that
    // the ID is user defined.
    public static void replace(Document document) {
        Set<String> documentKeys = document.keySet();
        if (documentKeys.contains(OBJECT_ID_PROPERTY)) {
            Object id = document.get(OBJECT_ID_PROPERTY);
            if (id instanceof ObjectId) {
                ObjectId theId = (ObjectId) id;
                document.put(OBJECT_ID_PROPERTY, of("$oid", theId.toHexString()));
            }
        }
    }
}
