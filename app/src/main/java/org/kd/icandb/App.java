package org.kd.icandb;

import org.kd.icandb.services.ICanDBService;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * @author kduborenko
 */
public class App {

    public static void main(String[] args) throws ICanDBException {
        ApplicationContext context = new AnnotationConfigApplicationContext(
                App.class.getPackage().getName());
        context.getBean(ICanDBService.class).start();
    }

}
