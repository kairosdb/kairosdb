package org.kairosdb.core.annotation;

import com.google.common.base.Defaults;
import org.apache.commons.lang3.ClassUtils;
import org.kairosdb.core.processingstage.metadata.QueryPropertyMetadata;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang.StringUtils.isEmpty;

public class AnnotationUtils
{
    @SuppressWarnings("ConstantConditions")
    public static List<QueryPropertyMetadata> getPropertyMetadata(Class clazz)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, ClassNotFoundException
    {
        checkNotNull(clazz, "class cannot be null");

        List<QueryPropertyMetadata> properties = new ArrayList<>();
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            if (field.getAnnotation(QueryProperty.class) != null) {
                String type = getType(field);
                String options = null;
                if (field.getType().isEnum()) {
                    options = getEnumAsString(field.getType());
                    type = "enum";
                }

                QueryProperty property = field.getAnnotation(QueryProperty.class);
                properties.add(new QueryPropertyMetadata(field.getName(), type, options,
                        isEmpty(property.default_value()) ? getDefaultValue(field) : property.default_value(),
                        property));
            }

            QueryCompoundProperty annotation = field.getAnnotation(QueryCompoundProperty.class);
            if (annotation != null) {
                properties.add(new QueryPropertyMetadata(field.getName(), annotation, getPropertyMetadata(field.getType())));
            }
        }

        if (clazz.getSuperclass() != null) {
            properties.addAll(getPropertyMetadata(clazz.getSuperclass()));
        }

        //noinspection Convert2Lambda
        properties.sort(new Comparator<QueryPropertyMetadata>()
        {
            @Override
            public int compare(QueryPropertyMetadata o1, QueryPropertyMetadata o2)
            {
                return o1.getLabel().compareTo(o2.getLabel());
            }
        });

        return properties;
    }

    private static String getEnumAsString(Class type)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException
    {
        StringBuilder builder = new StringBuilder();
        Field[] declaredFields = type.getDeclaredFields();
        for (Field declaredField : declaredFields) {
            if (declaredField.isEnumConstant()) {
                if (builder.length() > 0) {
                    builder.append(',');
                }
                builder.append(declaredField.getName());
            }
        }

        return builder.toString();
    }

    private static String getType(Field field)
    {
        if (Collection.class.isAssignableFrom(field.getType()) || field.getType().isArray())
        {
            return "array";
        }
        return field.getType().getSimpleName();
    }

    private static String getDefaultValue(Field field)
            throws ClassNotFoundException
    {
        if (field.getType().isAssignableFrom(String.class))
        {
            return "";
        }
        else if (Collection.class.isAssignableFrom(field.getType()) || field.getType().isArray())
        {
            return "[]";
        }
        else {
            return String.valueOf(Defaults.defaultValue(ClassUtils.getClass(field.getType().getSimpleName())));
        }
    }
}
