using System.Collections.Generic;

namespace TimAbell.MockableCosmos.Parsing;

/// <summary>
/// Represents a parsed CosmosDB SQL query.
/// </summary>
public class CosmosDbSqlQuery
{
    public SelectClause Select { get; }
    public FromClause From { get; }
    public WhereClause Where { get; }
    public OrderByClause OrderBy { get; }
    public LimitClause Limit { get; }

    public CosmosDbSqlQuery(
        SelectClause select,
        FromClause from,
        WhereClause where = null,
        OrderByClause orderBy = null,
        LimitClause limit = null)
    {
        Select = select;
        From = from;
        Where = where;
        OrderBy = orderBy;
        Limit = limit;
    }
}

/// <summary>
/// Represents a SELECT clause in a CosmosDB SQL query.
/// </summary>
public class SelectClause
{
    public IReadOnlyList<SelectItem> Items { get; }
    public bool IsSelectAll => Items.Count == 1 && Items[0] is SelectAllItem;

    public SelectClause(IReadOnlyList<SelectItem> items)
    {
        Items = items;
    }
}

/// <summary>
/// Base class for items in a SELECT clause.
/// </summary>
public abstract class SelectItem
{
}

/// <summary>
/// Represents a SELECT * item.
/// </summary>
public class SelectAllItem : SelectItem
{
    public static readonly SelectAllItem Instance = new SelectAllItem();
    public SelectAllItem() { }
}

/// <summary>
/// Represents a property reference in a SELECT clause.
/// </summary>
public class PropertySelectItem : SelectItem
{
    public string PropertyPath { get; }

    public PropertySelectItem(string propertyPath)
    {
        PropertyPath = propertyPath;
    }
}

/// <summary>
/// Represents a FROM clause in a CosmosDB SQL query.
/// </summary>
public class FromClause
{
    public string Source { get; }
    public string Alias { get; }

    public FromClause(string source, string alias)
    {
        Source = source;
        Alias = alias;
    }
}

/// <summary>
/// Represents a WHERE clause in a CosmosDB SQL query.
/// </summary>
public class WhereClause
{
    public Expression Condition { get; }

    public WhereClause(Expression condition)
    {
        Condition = condition;
    }
}

/// <summary>
/// Represents an ORDER BY clause in a CosmosDB SQL query.
/// </summary>
public class OrderByClause
{
    public IReadOnlyList<OrderByItem> Items { get; }

    public OrderByClause(IReadOnlyList<OrderByItem> items)
    {
        Items = items;
    }
}

/// <summary>
/// Represents an item in an ORDER BY clause.
/// </summary>
public class OrderByItem
{
    public string PropertyPath { get; }
    public bool Descending { get; }

    public OrderByItem(string propertyPath, bool descending = false)
    {
        PropertyPath = propertyPath;
        Descending = descending;
    }
}

/// <summary>
/// Represents a LIMIT clause in a CosmosDB SQL query.
/// </summary>
public class LimitClause
{
    public int Value { get; }

    public LimitClause(int value)
    {
        Value = value;
    }
}

/// <summary>
/// Base class for expressions in a CosmosDB SQL query.
/// </summary>
public abstract class Expression
{
}

/// <summary>
/// Represents a binary operation in a CosmosDB SQL query.
/// </summary>
public class BinaryExpression : Expression
{
    public Expression Left { get; }
    public BinaryOperator Operator { get; }
    public Expression Right { get; }

    public BinaryExpression(Expression left, BinaryOperator op, Expression right)
    {
        Left = left;
        Operator = op;
        Right = right;
    }
}

/// <summary>
/// Represents the operators available for binary expressions.
/// </summary>
public enum BinaryOperator
{
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    And,
    Or
}

/// <summary>
/// Represents a property access in a CosmosDB SQL query.
/// </summary>
public class PropertyExpression : Expression
{
    public string PropertyPath { get; }

    public PropertyExpression(string propertyPath)
    {
        PropertyPath = propertyPath;
    }
}

/// <summary>
/// Represents a constant value in a CosmosDB SQL query.
/// </summary>
public class ConstantExpression : Expression
{
    public object Value { get; }

    public ConstantExpression(object value)
    {
        Value = value;
    }
}

/// <summary>
/// Represents a function call in a CosmosDB SQL query.
/// </summary>
public class FunctionCallExpression : Expression
{
    public string FunctionName { get; }
    public string Name => FunctionName;
    public IReadOnlyList<Expression> Arguments { get; }

    public FunctionCallExpression(string functionName, IReadOnlyList<Expression> arguments)
    {
        FunctionName = functionName;
        Arguments = arguments;
    }
}
