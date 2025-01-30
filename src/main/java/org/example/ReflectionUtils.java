package org.example;

import org.apache.commons.lang3.reflect.FieldUtils;

public class ReflectionUtils {

    public static <T> T get(Object object, String fieldName) {
        try {
            return (T) FieldUtils.readField(object, fieldName, true);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
