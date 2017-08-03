package org.kairosdb.core.annotatedAggregator;


import com.google.inject.Inject;
import org.kairosdb.core.aggregator.Sampling;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.annotation.FeatureCompoundProperty;
import org.kairosdb.core.annotation.FeatureProperty;
import org.kairosdb.core.annotation.ValidationProperty;

@FeatureComponent(
        name = "A",
        description = "The A Aggregator"
)
public class AAggregator extends BAggregator
{
    @FeatureProperty(
            name = "allAnnotation",
            label = "AllAnnotation",
            description = "This is allAnnotation",
            optional = true,
            default_value = "2",
            validations = {
                    @ValidationProperty(
                            expression =  "value > 0",
                            message = "Value must be greater than 0."
                    )
            },
            type = "int"
    )
    private int allAnnotation;

    @FeatureProperty(
            label = "MyDouble",
            description = "This is myDouble"
    )
    private double myDouble;

    @FeatureProperty(
            label = "MyLong",
            description = "This is myLong"
    )
    private long myLong;

    @FeatureProperty(
            label = "MyInt",
            description = "This is myInt"
    )
    private int myInt;

   @FeatureProperty(
            label = "MyBoolean",
            description = "This is myBoolean"
    )
    private boolean myBoolean;

   @FeatureProperty(
            label = "MyString",
            description = "This is myString"
    )
    private String myString;

    @FeatureCompoundProperty(
            label = "Sampling",
            order = {"Value", "Unit"}
    )
    private Sampling m_sampling;

    private String unannotated;

    @Inject
    public AAggregator()
    {
    }

}
