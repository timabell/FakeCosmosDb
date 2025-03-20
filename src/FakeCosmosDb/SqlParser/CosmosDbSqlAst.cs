using System.Collections.Generic;

namespace TimAbell.FakeCosmosDb.SqlParser;

/// <summary>
/// Represents a parsed CosmosDB SQL query.
/// </summary>
public class CosmosDbSqlQuery(
	SelectClause select,
	FromClause from,
	WhereClause where = null,
	OrderByClause orderBy = null,
	LimitClause limit = null)
{
	public SelectClause Select { get; } = select;
	public FromClause From { get; } = from;
	public WhereClause Where { get; } = where;
	public OrderByClause OrderBy { get; } = orderBy;
	public LimitClause Limit { get; } = limit;

	public override string ToString()
	{
		var parts = new List<string>
		{
			Select.ToString(),
			From.ToString()
		};

		if (Where != null)
		{
			parts.Add(Where.ToString());
		}

		if (OrderBy != null)
		{
			parts.Add(OrderBy.ToString());
		}

		if (Limit != null)
		{
			parts.Add(Limit.ToString());
		}

		return string.Join(" ", parts);
	}
}

/// <summary>
/// Represents a SELECT clause in a CosmosDB SQL query.
/// </summary>
public class SelectClause(IReadOnlyList<SelectItem> items, TopClause top)
{
	public IReadOnlyList<SelectItem> Items { get; } = items;
	public bool IsSelectAll => Items.Count == 1 && Items[0] is SelectAllItem;
	public TopClause Top { get; } = top;

	public override string ToString()
	{
		var result = "SELECT ";

		if (Top != null)
		{
			result += $"{Top} ";
		}

		if (IsSelectAll)
		{
			result += "*";
		}
		else
		{
			var propertyPaths = new List<string>();
			foreach (var item in Items)
			{
				if (item is PropertySelectItem propertyItem)
				{
					propertyPaths.Add(propertyItem.PropertyPath);
				}
			}

			result += string.Join(", ", propertyPaths);
		}

		return result;
	}
}

/// <summary>
/// Base class for items in a SELECT clause.
/// </summary>
public abstract class SelectItem;

/// <summary>
/// Represents a SELECT * item.
/// </summary>
public class SelectAllItem : SelectItem;

/// <summary>
/// Represents a property reference in a SELECT clause.
/// </summary>
public class PropertySelectItem(string propertyPath) : SelectItem
{
	public string PropertyPath { get; } = propertyPath;
}

/// <summary>
/// Represents a FROM clause in a CosmosDB SQL query.
/// </summary>
public class FromClause(string source, string alias)
{
	public string Source { get; } = source;
	public string Alias { get; } = alias;

	public override string ToString()
	{
		return Alias != null ? $"FROM {Source} AS {Alias}" : $"FROM {Source}";
	}
}

/// <summary>
/// Represents a WHERE clause in a CosmosDB SQL query.
/// </summary>
public class WhereClause(Expression condition)
{
	public Expression Condition { get; } = condition;

	public override string ToString() => $"WHERE {Condition}";
}

/// <summary>
/// Represents an ORDER BY clause in a CosmosDB SQL query.
/// </summary>
public class OrderByClause(IReadOnlyList<OrderByItem> items)
{
	public IReadOnlyList<OrderByItem> Items { get; } = items;

	public override string ToString() => $"ORDER BY {string.Join(", ", Items)}";
}

/// <summary>
/// Represents an item in an ORDER BY clause.
/// </summary>
public class OrderByItem(string propertyPath, bool descending = false)
{
	public string PropertyPath { get; } = propertyPath;
	public bool Descending { get; } = descending;

	public override string ToString() => Descending ? $"{PropertyPath} DESC" : $"{PropertyPath} ASC";
}

/// <summary>
/// Represents a LIMIT clause in a CosmosDB SQL query.
/// </summary>
public class LimitClause(int value)
{
	public int Value { get; } = value;

	public override string ToString() => $"LIMIT {Value}";
}

/// <summary>
/// Represents a TOP clause in a CosmosDB SQL query.
/// </summary>
public class TopClause(int value)
{
	public int Value { get; } = value;

	public override string ToString() => $"TOP {Value}";
}

/// <summary>
/// Base class for expressions in a CosmosDB SQL query.
/// </summary>
public abstract class Expression
{
	public abstract override string ToString();
}

/// <summary>
/// Represents a binary operation in a CosmosDB SQL query.
/// </summary>
public class BinaryExpression(Expression left, BinaryOperator op, Expression right) : Expression
{
	public Expression Left { get; } = left;
	public BinaryOperator Operator { get; } = op;
	public Expression Right { get; } = right;

	public override string ToString() => $"({Left} {Operator} {Right})";
}

/// <summary>
/// Represents a unary operation in a CosmosDB SQL query.
/// </summary>
public class UnaryExpression(UnaryOperator op, Expression operand) : Expression
{
	public UnaryOperator Operator { get; } = op;
	public Expression Operand { get; } = operand;

	public override string ToString() => $"{Operator}({Operand})";
}

/// <summary>
/// Represents the operators available for unary expressions.
/// </summary>
public enum UnaryOperator
{
	Not
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
	Or,
	Between
}

/// <summary>
/// Represents a property access in a CosmosDB SQL query.
/// </summary>
public class PropertyExpression(string propertyPath) : Expression
{
	public string PropertyPath { get; } = propertyPath;

	public override string ToString() => PropertyPath;
}

/// <summary>
/// Represents a constant value in a CosmosDB SQL query.
/// </summary>
public class ConstantExpression(object value) : Expression
{
	public object Value { get; } = value;

	public override string ToString() => Value?.ToString() ?? "null";
}

/// <summary>
/// Represents a function call in a CosmosDB SQL query.
/// </summary>
public class FunctionCallExpression(string functionName, IReadOnlyList<Expression> arguments) : Expression
{
	public string FunctionName { get; } = functionName;
	public string Name => FunctionName;
	public IReadOnlyList<Expression> Arguments { get; } = arguments;

	public override string ToString() => $"{FunctionName}({string.Join(", ", Arguments)})";
}

/// <summary>
/// Represents a BETWEEN expression, which has a lower and upper bound.
/// </summary>
public class BetweenExpression(Expression lowerBound, Expression upperBound) : Expression
{
	public Expression LowerBound { get; } = lowerBound;
	public Expression UpperBound { get; } = upperBound;

	public override string ToString()
	{
		return $"BETWEEN {LowerBound} AND {UpperBound}";
	}
}

/// <summary>
/// Represents a parameter reference in a CosmosDB SQL query (e.g., @param).
/// </summary>
public class ParameterExpression(string parameterName) : Expression
{
	public string ParameterName { get; } = parameterName;

	public override string ToString() => ParameterName;
}
