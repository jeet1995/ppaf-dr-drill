package org.example;

import java.util.UUID;

public class Book {

    private String id;

    private String pk;

    private String foo0, foo1, foo2, foo3, foo4, foo5, foo6, foo7, foo8, foo9;

    public Book() {
    }

    public Book(String id, String pk, String foo0, String foo1, String foo2, String foo3, String foo4, String foo5, String foo6, String foo7, String foo8, String foo9) {
        this.id = id;
        this.pk = pk;
        this.foo0 = foo0;
        this.foo1 = foo1;
        this.foo2 = foo2;
        this.foo3 = foo3;
        this.foo4 = foo4;
        this.foo5 = foo5;
        this.foo6 = foo6;
        this.foo7 = foo7;
        this.foo8 = foo8;
        this.foo9 = foo9;
    }

    public static Book build() {
        String guid = UUID.randomUUID().toString();

        return new Book(
                guid,
                guid,
                guid,
                guid,
                guid,
                guid,
                guid,
                guid,
                guid,
                guid,
                guid,
                guid);
    }

    public static Book build(String id) {
        return new Book(id, id, id, id, id, id, id, id, id, id, id, id);
    }

    public String getId() {
        return id;
    }

    public String getPk() {
        return pk;
    }

    public String getFoo0() {
        return foo0;
    }

    public String getFoo1() {
        return foo1;
    }

    public String getFoo2() {
        return foo2;
    }

    public String getFoo3() {
        return foo3;
    }

    public String getFoo4() {
        return foo4;
    }

    public String getFoo5() {
        return foo5;
    }

    public String getFoo6() {
        return foo6;
    }

    public String getFoo7() {
        return foo7;
    }

    public String getFoo8() {
        return foo8;
    }

    public String getFoo9() {
        return foo9;
    }
}
