package org.kairosdb.core.annotatedAggregator;


import com.google.inject.Inject;
import org.kairosdb.core.aggregator.Sampling;
import org.kairosdb.core.annotation.QueryCompoundProperty;
import org.kairosdb.core.annotation.QueryProcessor;
import org.kairosdb.core.annotation.QueryProperty;
import org.kairosdb.core.annotation.ValidationProperty;

@QueryProcessor(
        name = "A",
        description = "The A Aggregator"
)
public class AAggregator extends BAggregator
{
    @QueryProperty(
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

    @QueryProperty(
            label = "MyDouble",
            description = "This is myDouble"
    )
    private double myDouble;

    @QueryProperty(
            label = "MyLong",
            description = "This is myLong"
    )
    private long myLong;

    @QueryProperty(
            label = "MyInt",
            description = "This is myInt"
    )
    private int myInt;

   @QueryProperty(
            label = "MyBoolean",
            description = "This is myBoolean"
    )
    private boolean myBoolean;

   @QueryProperty(
            label = "MyString",
            description = "This is myString"
    )
    private String myString;

    @QueryCompoundProperty(
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
