package org.kairosdb.core.processingstage;

import com.google.common.collect.ImmutableList;
import org.kairosdb.core.processingstage.metadata.FeatureProcessorMetadata;

public interface FeatureProcessingFactory<Feature>
{
    /**
     * Create new instance of a feature processor.
     *
     * @param   name    name of the feature processor
     * @return          created instance of the feature processor
     */
    Feature createFeatureProcessor(String name);

    /**
     * Returns the feature class.
     *
     * @return feature class
     */
    Class<Feature> getFeature();

    /**
     * Returns an {@link ImmutableList} of {@link FeatureProcessorMetadata}
     * describing the feature processor.
     *
     * @return the {@link ImmutableList} describing the feature processor
     */
    ImmutableList<FeatureProcessorMetadata> getFeatureProcessorMetadata();
}
