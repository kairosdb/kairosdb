package org.kairosdb.core.annotatedAggregator;


import com.google.inject.Inject;
import org.kairosdb.core.aggregator.Sampling;
import org.kairosdb.core.aggregator.annotation.AggregatorCompoundProperty;
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.aggregator.annotation.AggregatorProperty;

@AggregatorName(
        name = "A",
        description = "The A Aggregator"
)
public class AAggregator extends BAggregator
{
    @AggregatorProperty(
            name = "allAnnotation",
            label = "AllAnnotation",
            description = "This is allAnnotation",
            optional = true,
            default_value = "2",
            validation = "value > 0",
            type = "int"
    )
    private int allAnnotation;

    @AggregatorProperty(
            label = "MyDouble",
            description = "This is myDouble"
    )
    private double myDouble;

    @AggregatorProperty(
            label = "MyLong",
            description = "This is myLong"
    )
    private long myLong;

    @AggregatorProperty(
            label = "MyInt",
            description = "This is myInt"
    )
    private int myInt;

   @AggregatorProperty(
            label = "MyBoolean",
            description = "This is myBoolean"
    )
    private boolean myBoolean;

   @AggregatorProperty(
            label = "MyString",
            description = "This is myString"
    )
    private String myString;

    @AggregatorCompoundProperty(
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
