package org.kairosdb.core.aggregator.json;

public class ValidationMetadata
{
    private String expression;
    private String type;
    private String message;

    public ValidationMetadata(String expression, String type, String message)
    {
        this.expression = expression;
        this.type = type;
        this.message = message;
    }

    public String getExpression() { return expression; }
    public String getType() { return type; }
    public String getMessage() { return message; }
}