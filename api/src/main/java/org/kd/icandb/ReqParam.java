package org.kd.icandb;

import java.lang.annotation.*;

/**
 * @author Kiryl Dubarenka
 */
@Target({ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ReqParam {
    String value();
}
