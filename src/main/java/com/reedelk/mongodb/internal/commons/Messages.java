package com.reedelk.mongodb.internal.commons;

public class Messages {

    private Messages() {
    }

    interface FormattedMessage {

        String template();

        default String format(Object ...args) {
            return String.format(template(), args);
        }
    }

    public enum Insert implements FormattedMessage {

        INSERT_DOCUMENT_EMPTY("The document to insert was null. Null documents cannot be inserted into MongoDB, did you mean to insert an empty document ({}) ? (DynamicValue=[%s]).");

        private String msg;

        Insert(String msg) {
            this.msg = msg;
        }

        @Override
        public String template() {
            return msg;
        }
    }
}
