package org.kairosdb.core.processingstage;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binding;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.kairosdb.core.annotation.QueryProcessor;
import org.kairosdb.core.processingstage.metadata.QueryProcessorMetadata;
import org.kairosdb.core.processingstage.metadata.QueryPropertyMetadata;

import javax.validation.constraints.NotNull;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static org.kairosdb.core.annotation.AnnotationUtils.getPropertyMetadata;

public abstract class GenericQueryProcessingStageFactory<QueryProcessorFamily> implements QueryProcessingStageFactory<QueryProcessorFamily>
{
    private Class<QueryProcessorFamily> queryProcessorFamily;
    protected Map<String, Class<QueryProcessorFamily>> queryProcessors = new HashMap<>();
    protected List<QueryProcessorMetadata> queryProcessorMetadata = new ArrayList<>();
    protected Injector injector;

    /**
     * Constructor of a generic class to easily generate a processing stage factory.
     *
     * @param injector                      Guice {@link Injector} instance needed for binding
     * @param queryProcessorFamily          query processor family class
     */
    @SuppressWarnings("unchecked")
    protected GenericQueryProcessingStageFactory(@NotNull Injector injector, @NotNull Class<QueryProcessorFamily> queryProcessorFamily)
            throws InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IllegalAccessException
    {
        this.injector = injector;
        this.queryProcessorFamily = queryProcessorFamily;
        Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

        for (Key<?> key : bindings.keySet())
        {
            Class<?> bindingClass = key.getTypeLiteral().getRawType();

            if (queryProcessorFamily.isAssignableFrom(bindingClass))
            {
                QueryProcessor annotation = bindingClass.getAnnotation(QueryProcessor.class);
                if (annotation == null)
                    throw new IllegalStateException("Processor class " + bindingClass.getName() +
                            " does not have required annotation " + QueryProcessor.class.getName());

                queryProcessors.put(annotation.name(), (Class<QueryProcessorFamily>) bindingClass);
                List<QueryPropertyMetadata> properties = getPropertyMetadata(bindingClass);
                queryProcessorMetadata.add(new QueryProcessorMetadata(annotation.name(), labelizeQueryProcessor(annotation), annotation.description(), properties));
            }
        }
        queryProcessorMetadata.sort(Comparator.comparing(QueryProcessorMetadata::getName));
    }

    @Override
    public Class<QueryProcessorFamily> getQueryProcessorFamily() { return queryProcessorFamily; }

    @Override
    public ImmutableList<QueryProcessorMetadata> getQueryProcessorMetadata()
    {
        return new ImmutableList.Builder<QueryProcessorMetadata>().addAll(queryProcessorMetadata).build();
    }

    @Override
    public QueryProcessorFamily createQueryProcessor(String name)
    {
        Class<QueryProcessorFamily> processClass = queryProcessors.get(name);

        if (processClass == null)
            return (null);
        return (injector.getInstance(processClass));
    }

    private String labelizeQueryProcessor(QueryProcessor annotation)
    {
        if (!annotation.label().isEmpty())
            return annotation.label();

        StringBuilder label = new StringBuilder();
        for (String word : annotation.name().toLowerCase().split("_"))
        {
            label.append(word.substring(0, 1).toUpperCase());
            label.append(word.substring(1));
            label.append(" ");
        }
        return label.toString().trim();
    }
}
