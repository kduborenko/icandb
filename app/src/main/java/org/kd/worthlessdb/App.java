package org.kd.worthlessdb;

import org.kd.worthlessdb.services.WorthlessDbService;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * @author kduborenko
 */
public class App {

    public static void main(String[] args) throws WorthlessDBException {
        ApplicationContext context = new AnnotationConfigApplicationContext(
                App.class.getPackage().getName());
        context.getBean(WorthlessDbService.class).start();
    }

}
