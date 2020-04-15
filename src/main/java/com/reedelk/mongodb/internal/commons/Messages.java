package com.reedelk.mongodb.internal.commons;

import com.reedelk.runtime.api.commons.FormattedMessage;

public class Messages {

    private Messages() {
    }

    public enum Insert implements FormattedMessage {

        INSERT_DOCUMENT_EMPTY("The document to insert was null. " +
                "Null documents cannot be inserted into MongoDB, " +
                "did you mean to insert an empty document ({}) ? (DynamicValue=[%s]).");

        private String message;

        Insert(String message) {
            this.message = message;
        }

        @Override
        public String template() {
            return message;
        }
    }

    public enum Find implements FormattedMessage {

        FIND_FILTER_NULL("The find filter was null. " +
                "I cannot execute find operation with a null filter (DynamicValue=[%s]).");

        private String message;

        Find(String message) {
            this.message = message;
        }

        @Override
        public String template() {
            return message;
        }
    }
}
