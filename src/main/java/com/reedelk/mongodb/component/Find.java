package com.reedelk.mongodb.component;

import com.reedelk.runtime.api.annotation.ModuleComponent;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ServiceScope;

@ModuleComponent("MongoDB Find")
@Component(service = Find.class, scope = ServiceScope.PROTOTYPE)
public class Find implements ProcessorSync {

    @Override
    public Message apply(FlowContext flowContext, Message message) {
        return null;
    }
}
