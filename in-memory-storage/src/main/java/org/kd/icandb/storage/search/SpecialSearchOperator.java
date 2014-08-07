package org.kd.icandb.storage.search;

/**
 * @author Kiryl Dubarenka
 */
public abstract class SpecialSearchOperator<T> implements SearchOperator {

    private T params;

    public SpecialSearchOperator(T params) {
        this.params = params;
    }

    public T getParams() {
        return params;
    }
}
