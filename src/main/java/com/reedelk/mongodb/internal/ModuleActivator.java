package com.reedelk.mongodb.internal;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;

import static org.osgi.service.component.annotations.ServiceScope.SINGLETON;

@Component(service = ModuleActivator.class, scope = SINGLETON, immediate = true)
public class ModuleActivator {

    @Reference
    private ClientFactory clientFactory;

    @Deactivate
    public void deactivate() {
        // All the MongoDB client connections should be closed, to make sure
        // that nothing has been left open.
        clientFactory.dispose();
    }
}
