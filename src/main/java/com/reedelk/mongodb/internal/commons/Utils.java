package com.reedelk.mongodb.internal.commons;

public class Utils {

    private Utils() {
    }

    public static boolean isTrue(Boolean value) {
        return value != null && value;
    }

    public static String getClassOrNull(Object object) {
        return object == null ? null : object.getClass().getName();
    }
}
