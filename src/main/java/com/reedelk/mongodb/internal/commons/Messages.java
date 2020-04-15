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

        FIND_FILTER_NULL("The Find filter was null. " +
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

    public enum Update implements FormattedMessage {

        UPDATE_FILTER_NULL("The Update filter was null. " +
                "I cannot execute Update operation with a null filter (DynamicValue=[%s])."),

        UPDATE_DOCUMENT_EMPTY("The updated document was null. " +
                                      "Null documents cannot be updated into MongoDB, " +
                                      "did you mean to update with an empty document ({}) ? (DynamicValue=[%s]).");

        private String message;

        Update(String message) {
            this.message = message;
        }

        @Override
        public String template() {
            return message;
        }
    }

    public enum Delete implements FormattedMessage {

        DELETE_FILTER_NULL("The Delete filter was null. " +
                "I cannot execute Delete operation with a null filter (DynamicValue=[%s]).");

        private String message;

        Delete(String message) {
            this.message = message;
        }

        @Override
        public String template() {
            return message;
        }
    }

    public enum Count implements FormattedMessage {

        COUNT_FILTER_NULL("The Count filter was null. " +
                "I cannot execute Count operation with a null filter (DynamicValue=[%s]).");

        private String message;

        Count(String message) {
            this.message = message;
        }

        @Override
        public String template() {
            return message;
        }
    }
}
