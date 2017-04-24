package org.kairosdb.core.processingstage.metadata;

public class QueryValidationMetadata
{
    private String expression;
    private String type;
    private String message;

    public QueryValidationMetadata(String expression, String type, String message)
    {
        this.expression = expression;
        this.type = type;
        this.message = message;
    }

    public String getExpression() { return expression; }
    public String getType() { return type; }
    public String getMessage() { return message; }
}