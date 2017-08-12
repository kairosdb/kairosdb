package org.kairosdb.core.annotation;

import org.junit.Test;

import java.lang.reflect.InvocationTargetException;

public class AnnotationUtilsTest
{
    @Test(expected = NullPointerException.class)
    public void test_getPropertyMetadata_nullClass_invalid()
            throws InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IllegalAccessException
    {
        AnnotationUtils.getPropertyMetadata(null);
    }
}