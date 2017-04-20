package org.kairosdb.core.process;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binding;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.kairosdb.core.aggregator.json.QueryMetadata;
import org.kairosdb.core.aggregator.json.QueryPropertyMetadata;
import org.kairosdb.core.annotation.ProcessProperty;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static org.kairosdb.core.annotation.AnnotationUtils.getPropertyMetadata;

public class GenericProcessFactory<ProcessType> implements ProcessFactory<ProcessType>
{
    private Map<String, Class<ProcessType>> processes = new HashMap<String, Class<ProcessType>>();
    private List<QueryMetadata> queryMetadata = new ArrayList<QueryMetadata>();
    private Injector injector;

    @SuppressWarnings("unchecked")
    public GenericProcessFactory(Injector injector, Class<ProcessType> processType)
            throws InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IllegalAccessException
    {
        this.injector = injector;
        Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

        for (Key<?> key : bindings.keySet())
        {
            Class<?> bindingClass = key.getTypeLiteral().getRawType();

            if (processType.isAssignableFrom(bindingClass))
            {
                ProcessProperty annotation = bindingClass.getAnnotation(ProcessProperty.class);
                if (annotation == null)
                    throw new IllegalStateException("Aggregator class " + bindingClass.getName() +
                            " does not have required annotation " + ProcessProperty.class.getName());

                processes.put(annotation.name(), (Class<ProcessType>) bindingClass);
                List<QueryPropertyMetadata> properties = getPropertyMetadata(bindingClass);
                queryMetadata.add(new QueryMetadata(annotation.name(), labelizeProcess(annotation), annotation.description(), properties));
            }
            Collections.sort(queryMetadata, new Comparator<QueryMetadata>()
            {
                @Override
                public int compare(QueryMetadata o1, QueryMetadata o2) { return o1.getName().compareTo(o2.getName()); }
            });
        }
    }

    @Override
    public ProcessType createProcess(String name)
    {
        Class<ProcessType> processClass = processes.get(name);

        if (processClass == null)
            return (null);
        ProcessType process = injector.getInstance(processClass);
        return (process);
    }

    @Override
    public ImmutableList<QueryMetadata> getQueryMetadata()
    {
        return new ImmutableList.Builder<QueryMetadata>().addAll(queryMetadata).build();
    }

    private String labelizeProcess(ProcessProperty annotation)
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
