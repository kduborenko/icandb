package org.kd.icandb.driver;

import org.kd.icandb.ICanDB;
import org.kd.icandb.ICanDBDriver;
import org.kd.icandb.ICanDBDriverBuilder;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * @author Kiryl Dubarenka
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
