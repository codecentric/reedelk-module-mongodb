package com.reedelk.mongodb.internal.commons;

import com.reedelk.runtime.api.exception.PlatformException;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicvalue.DynamicObject;

import java.util.function.Supplier;

import static com.reedelk.runtime.api.commons.DynamicValueUtils.isNullOrBlank;

public class Utils {

    private Utils() {
    }

    public static boolean isTrue(Boolean value) {
        return value != null && value;
    }

    public static String getClassOrNull(Object object) {
        return object == null ? null : object.getClass().getName();
    }

    public static Object evaluateOrUsePayloadWhenEmpty(DynamicObject dynamicValue,
                                                       ScriptEngineService scriptEngine,
                                                       FlowContext context,
                                                       Message message,
                                                       Supplier<? extends PlatformException> exception) {
        // If the dynamic expression is null or blank, it means we are using the default value,
        // which is the message payload. If the payload
        return isNullOrBlank(dynamicValue) ?
                message.payload() :
                scriptEngine.evaluate(dynamicValue, context, message).orElseThrow(exception);
    }
}
