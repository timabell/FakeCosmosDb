using System.Collections.Generic;

namespace TimAbell.FakeCosmosDb.SqlParser;

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
public class SelectClause
{
	public IReadOnlyList<SelectItem> Items { get; }
	public bool IsSelectAll => Items.Count == 1 && Items[0] is SelectAllItem;
	public TopClause Top { get; }

	public SelectClause(IReadOnlyList<SelectItem> items)
	{
		Items = items;
	}

	public SelectClause(IReadOnlyList<SelectItem> items, TopClause top)
	{
		Items = items;
		Top = top;
	}

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

	public override string ToString()
	{
		return Alias != null ? $"FROM {Source} AS {Alias}" : $"FROM {Source}";
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

	public override string ToString() => $"WHERE {Condition}";
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

	public override string ToString() => $"ORDER BY {string.Join(", ", Items)}";
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

	public override string ToString() => Descending ? $"{PropertyPath} DESC" : $"{PropertyPath} ASC";
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

	public override string ToString() => $"LIMIT {Value}";
}

/// <summary>
/// Represents a TOP clause in a CosmosDB SQL query.
/// </summary>
public class TopClause
{
	public int Value { get; }

	public TopClause(int value)
	{
		Value = value;
	}

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

	public override string ToString() => $"({Left} {Operator} {Right})";
}

/// <summary>
/// Represents a unary operation in a CosmosDB SQL query.
/// </summary>
public class UnaryExpression : Expression
{
	public UnaryOperator Operator { get; }
	public Expression Operand { get; }

	public UnaryExpression(UnaryOperator op, Expression operand)
	{
		Operator = op;
		Operand = operand;
	}

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

	public override string ToString() => PropertyPath;
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

	public override string ToString() => Value?.ToString() ?? "null";
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

	public override string ToString() => $"{FunctionName}({string.Join(", ", Arguments)})";
}
