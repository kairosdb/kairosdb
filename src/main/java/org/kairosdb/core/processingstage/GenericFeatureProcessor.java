package org.kairosdb.core.processingstage;

import com.google.common.collect.ImmutableList;
import org.kairosdb.core.annotation.Feature;
import org.kairosdb.core.processingstage.metadata.FeatureProcessingMetadata;
import org.kairosdb.core.processingstage.metadata.FeatureProcessorMetadata;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;

public abstract class GenericFeatureProcessor implements FeatureProcessor
{
    private List<FeatureProcessingFactory<?>> featureProcessingFactories = new ArrayList<>();
    private List<FeatureProcessingMetadata> featureProcessingMetadata = new ArrayList<>();

    /**
     * Constructor of a generic class to easily generate a feature processor.
     *
     * @param featureProcessingFactories   list of {@link FeatureProcessingFactory}
     * @return instances composing the feature processor
     */
    protected GenericFeatureProcessor(@NotNull List<FeatureProcessingFactory<?>> featureProcessingFactories)
    {
        if (featureProcessingFactories.size() == 0)
            throw new IllegalArgumentException("featureProcessingFactories parameter can't be empty");
        for (int i = 0; i < featureProcessingFactories.size(); i++)
        {
            FeatureProcessingFactory<?> factory = featureProcessingFactories.get(i);
            ArrayList<FeatureProcessorMetadata> featureProcessorMetadata = new ArrayList<>();

            Feature annotation = factory.getClass().getAnnotation(Feature.class);
            if (annotation == null)
                throw new IllegalStateException("Feature class " + factory.getClass().getName() +
                        " does not have required annotation " + Feature.class.getName());
            if (factory.getFeatureProcessorMetadata() == null)
                throw new IllegalStateException("Feature processor class " + factory.getClass().getName() +
                        " does not have feature processor metadata");

            this.featureProcessingFactories.add(i, factory);
            featureProcessorMetadata.addAll(factory.getFeatureProcessorMetadata());
            this.featureProcessingMetadata.add(new FeatureProcessingMetadata(annotation.name(), annotation.label(), featureProcessorMetadata));
        }
    }

    @Override
    public ImmutableList<FeatureProcessingFactory<?>> getFeatureProcessingFactories()
    {
        return new ImmutableList.Builder<FeatureProcessingFactory<?>>().addAll(featureProcessingFactories).build();
    }

    @Override
    public FeatureProcessingFactory<?> getFeatureProcessingFactory(Class<?> feature)
    {
        for (FeatureProcessingFactory<?> factory : featureProcessingFactories)
            if (factory.getFeature() == feature)
                return factory;
        return null;
    }

    @Override
    public FeatureProcessingFactory<?> getFeatureProcessingFactory(String feature)
    {
        for (FeatureProcessingFactory<?> factory : featureProcessingFactories)
        {
            String factoryName = factory.getClass().getAnnotation(Feature.class).name();
            if (factoryName.equalsIgnoreCase(feature))
                return factory;
        }
        return null;
    }

    @Override
    public ImmutableList<FeatureProcessingMetadata> getFeatureProcessingMetadata()
    {
        return new ImmutableList.Builder<FeatureProcessingMetadata>().addAll(featureProcessingMetadata).build();
    }
}
