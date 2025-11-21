package org.kairosdb.core.processingstage.metadata;

public class FeatureValidationMetadata
{
    private final String expression;
    private final String type;
    private final String message;

    public FeatureValidationMetadata(String expression, String type, String message)
    {
        this.expression = expression;
        this.type = type;
        this.message = message;
    }

    public String getExpression() { return expression; }
    public String getType() { return type; }
    public String getMessage() { return message; }
}