package org.kd.icandb;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * @author kirk
 */
public class InMemoryDriverBuilder implements ICanDBDriverBuilder {

    @Override
    public String getProtocol() {
        return "mem";
    }

    @Override
    public ICanDB getDriver(String connectionString) {
        return new AnnotationConfigApplicationContext(
                ICanDBDriver.class.getPackage().getName()
        ).getBean(ICanDB.class);
    }
}
