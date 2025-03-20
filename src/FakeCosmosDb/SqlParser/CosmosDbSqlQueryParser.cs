using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace TimAbell.FakeCosmosDb.SqlParser;

public class CosmosDbSqlQueryParser()
{
	/// <summary>
	/// Parses a CosmosDB SQL query using the Sprache grammar.
	/// </summary>
	public ParsedQuery Parse(string query)
	{
		// First try to parse with the grammar
		var parsedQuery = CosmosDbSqlGrammar.ParseQuery(query);

		var result = ConvertToLegacyParsedQuery(parsedQuery);

		// If ORDER BY or LIMIT wasn't parsed correctly, try direct string parsing as fallback
		if ((query.Contains("ORDER BY", StringComparison.OrdinalIgnoreCase) && result.OrderBy == null) ||
			(query.Contains("LIMIT", StringComparison.OrdinalIgnoreCase) && result.Limit == 0))
		{
			FallbackParseOrderByAndLimit(query, result);
		}

		return result;
	}

	/// <summary>
	/// Dumps the current state of the parser for diagnostic purposes.
	/// </summary>
	public string DumpDebugInfo(string query)
	{
		try
		{
			var sb = new System.Text.StringBuilder();
			sb.AppendLine($"Original query: {query}");

			var parsedQuery = CosmosDbSqlGrammar.ParseQuery(query);
			sb.AppendLine($"AST: {parsedQuery}");

			var legacyQuery = ConvertToLegacyParsedQuery(parsedQuery);
			sb.AppendLine("\nLegacy ParsedQuery:");
			sb.AppendLine($"- FromName: {legacyQuery.FromName}");
			sb.AppendLine($"- FromAlias: {legacyQuery.FromAlias}");
			sb.AppendLine($"- PropertyPaths: {string.Join(", ", legacyQuery.PropertyPaths)}");

			if (legacyQuery.WhereConditions != null)
			{
				sb.AppendLine($"- WhereConditions ({legacyQuery.WhereConditions.Count}):");
				foreach (var condition in legacyQuery.WhereConditions)
				{
					sb.AppendLine($"  * {condition.PropertyPath} {condition.Operator} {condition.Value} (Type: {condition.Value?.Type})");
				}
			}
			else
			{
				sb.AppendLine("- WhereConditions: null");
			}

			if (legacyQuery.OrderBy != null)
			{
				sb.AppendLine($"- OrderBy ({legacyQuery.OrderBy.Count}):");
				foreach (var order in legacyQuery.OrderBy)
				{
					sb.AppendLine($"  * {order.PropertyPath} {order.Direction}");
				}
			}
			else
			{
				sb.AppendLine("- OrderBy: null");
			}

			sb.AppendLine($"- Limit: {legacyQuery.Limit}");
			sb.AppendLine($"- TopValue: {legacyQuery.TopValue}");

			return sb.ToString();
		}
		catch (Exception ex)
		{
			return $"Error dumping debug info: {ex.Message}";
		}
	}

	/// <summary>
	/// Converts the new AST-based parsed query to the legacy ParsedQuery format
	/// for compatibility with existing code.
	/// </summary>
	private ParsedQuery ConvertToLegacyParsedQuery(CosmosDbSqlQuery query) // todo: eliminate
	{
		var result = new ParsedQuery
		{
			SprachedSqlAst = query,
			PropertyPaths = ExtractPropertyPaths(query.Select),
			FromName = query.From.Source,
			FromAlias = query.From.Alias
		};

		// Extract TOP value
		if (query.Select.Top != null)
		{
			result.TopValue = query.Select.Top.Value;
		}

		// Extract WHERE conditions
		if (query.Where != null)
		{
			var whereConditions = ExtractWhereConditions(query.Where.Condition);
			if (whereConditions.Count > 0)
			{
				result.WhereConditions = whereConditions;
			}

			result.WhereExpression = query.Where.Condition;
		}

		// Extract ORDER BY
		if (query.OrderBy != null && query.OrderBy.Items.Count > 0)
		{
			var orderBy = ExtractOrderBy(query.OrderBy);
			if (orderBy != null && orderBy.Count > 0)
			{
				result.OrderBy = orderBy;
			}
		}

		// Extract LIMIT
		if (query.Limit != null)
		{
			result.Limit = query.Limit.Value;
		}

		return result;
	}

	/// <summary>
	/// Extracts property paths from the SELECT clause.
	/// </summary>
	private List<string> ExtractPropertyPaths(SelectClause select)
	{
		var result = new List<string>();

		foreach (var item in select.Items)
		{
			if (item is SelectAllItem)
			{
				result.Add("*");
			}
			else if (item is PropertySelectItem propertyItem)
			{
				result.Add(propertyItem.PropertyPath);
			}
		}

		return result;
	}

	/// <summary>
	/// Extracts order by information from the ORDER BY clause.
	/// </summary>
	private List<OrderInfo> ExtractOrderBy(OrderByClause orderBy)
	{
		if (orderBy == null || orderBy.Items == null || orderBy.Items.Count == 0)
		{
			return null;
		}

		return orderBy.Items
			.Select(item => new OrderInfo
			{
				PropertyPath = item.PropertyPath,
				Direction = item.Descending ? SortDirection.Descending : SortDirection.Ascending
			})
			.ToList();
	}

	/// <summary>
	/// Recursively extracts where conditions from the WHERE expression.
	/// </summary>
	private List<WhereCondition> ExtractWhereConditions(Expression condition)
	{
		var conditions = new List<WhereCondition>();

		if (condition == null)
		{
			return conditions;
		}

		// If it's a binary AND expression, collect conditions from both sides
		if (condition is BinaryExpression binaryExpr && binaryExpr.Operator == BinaryOperator.And)
		{
			conditions.AddRange(ExtractWhereConditions(binaryExpr.Left));
			conditions.AddRange(ExtractWhereConditions(binaryExpr.Right));
		}
		// If it's a binary OR expression, we keep the left side only for compatibility with the original parser
		// (original parser didn't support OR conditions properly)
		else if (condition is BinaryExpression orExpr && orExpr.Operator == BinaryOperator.Or)
		{
			conditions.AddRange(ExtractWhereConditions(orExpr.Left));
			// Ignore the right side in an OR condition
		}

		// If it's a comparison (e.g., Property > Value)
		if (condition is BinaryExpression comparison)
		{
			// Handle special case for BETWEEN operator
			if (comparison.Operator == BinaryOperator.Between)
			{
				if (comparison.Left is PropertyExpression betweenProp && comparison.Right is BetweenExpression betweenExpr)
				{
					// For BETWEEN operator with two constant values
					if (betweenExpr.LowerBound is ConstantExpression lowerConstExpr &&
						betweenExpr.UpperBound is ConstantExpression upperConstExpr)
					{
						// Create a single BETWEEN condition
						conditions.Add(new WhereCondition
						{
							PropertyPath = betweenProp.PropertyPath,
							Operator = ComparisonOperator.Between,
							Value = JToken.FromObject(new object[] { lowerConstExpr.Value, upperConstExpr.Value })
						});
					}
				}
			}
			else if (comparison.Left is PropertyExpression leftProp && comparison.Right is ConstantExpression rightConst)
			{
				conditions.Add(new WhereCondition
				{
					PropertyPath = leftProp.PropertyPath,
					Operator = GetComparisonOperator(comparison.Operator),
					Value = JToken.FromObject(rightConst.Value)
				});
			}
			else if (comparison.Left is PropertyExpression leftPropParam && comparison.Right is ParameterExpression rightParam)
			{
				// Handle property = @parameter
				conditions.Add(new WhereCondition
				{
					PropertyPath = leftPropParam.PropertyPath,
					Operator = GetComparisonOperator(comparison.Operator),
					ParameterName = rightParam.ParameterName
				});
			}
			else if (comparison.Right is PropertyExpression rightProp && comparison.Left is ConstantExpression leftConst)
			{
				// Handle reverse order (value = property)
				var reversedOp = GetReversedOperator(comparison.Operator);
				conditions.Add(new WhereCondition
				{
					PropertyPath = rightProp.PropertyPath,
					Operator = GetComparisonOperator(reversedOp),
					Value = JToken.FromObject(leftConst.Value)
				});
			}
			else if (comparison.Right is PropertyExpression rightPropParam && comparison.Left is ParameterExpression leftParam)
			{
				// Handle reverse order (@parameter = property)
				var reversedOp = GetReversedOperator(comparison.Operator);
				conditions.Add(new WhereCondition
				{
					PropertyPath = rightPropParam.PropertyPath,
					Operator = GetComparisonOperator(reversedOp),
					ParameterName = leftParam.ParameterName
				});
			}
		}
		// If it's a function call like CONTAINS or STARTSWITH
		else if (condition is FunctionCallExpression functionCall)
		{
			// Extract function name
			string functionName = functionCall.FunctionName.ToUpperInvariant();

			if ((functionName == "CONTAINS" || functionName == "STARTSWITH") &&
				functionCall.Arguments.Count >= 2 &&
				functionCall.Arguments[0] is PropertyExpression propExpr &&
				functionCall.Arguments[1] is ConstantExpression constExpr)
			{
				var op = functionName == "CONTAINS" ? ComparisonOperator.StringContains : ComparisonOperator.StringStartsWith;

				var whereCondition = new WhereCondition
				{
					PropertyPath = propExpr.PropertyPath,
					Operator = op,
					Value = JToken.FromObject(constExpr.Value)
				};

				// For CONTAINS with 3 arguments, the third is a boolean for case-insensitivity
				if (functionName == "CONTAINS" && functionCall.Arguments.Count == 3 &&
					functionCall.Arguments[2] is ConstantExpression caseInsensitiveArg &&
					caseInsensitiveArg.Value is bool ignoreCase)
				{
					whereCondition.IgnoreCase = ignoreCase;
				}

				conditions.Add(whereCondition);
			}
			else if (functionName == "IS_NULL" &&
					 functionCall.Arguments.Count == 1 &&
					 functionCall.Arguments[0] is PropertyExpression propNullExpr)
			{
				conditions.Add(new WhereCondition
				{
					PropertyPath = propNullExpr.PropertyPath,
					Operator = ComparisonOperator.Equals,
					Value = JValue.CreateNull()
				});
			}
			else if (functionName == "IS_DEFINED" &&
					 functionCall.Arguments.Count == 1 &&
					 functionCall.Arguments[0] is PropertyExpression propDefinedExpr)
			{
				conditions.Add(new WhereCondition
				{
					PropertyPath = propDefinedExpr.PropertyPath,
					Operator = ComparisonOperator.IsDefined,
					// Value can be null here since we're just checking if the property exists
					Value = JValue.CreateNull()
				});
			}
			else if (functionName == "ARRAY_CONTAINS" &&
					 functionCall.Arguments.Count == 2 &&
					 functionCall.Arguments[0] is PropertyExpression propArrayExpr &&
					 functionCall.Arguments[1] is ConstantExpression constArrayExpr)
			{
				conditions.Add(new WhereCondition
				{
					PropertyPath = propArrayExpr.PropertyPath,
					Operator = ComparisonOperator.ArrayContains,
					Value = constArrayExpr.Value != null ? JToken.FromObject(constArrayExpr.Value) : JValue.CreateNull()
				});
			}
		}
		// If it's just a property expression (rare, but can happen)
		else if (condition is PropertyExpression propExpr)
		{
			conditions.Add(new WhereCondition
			{
				PropertyPath = propExpr.PropertyPath,
				Operator = ComparisonOperator.Equals,
				Value = JToken.FromObject(true) // Treat as a boolean condition (property = true)
			});
		}

		return conditions;
	}

	/// <summary>
	/// Reverses a binary operator for reversed operand order
	/// </summary>
	private static BinaryOperator GetReversedOperator(BinaryOperator op)
	{
		return op switch
		{
			BinaryOperator.GreaterThan => BinaryOperator.LessThan,
			BinaryOperator.LessThan => BinaryOperator.GreaterThan,
			BinaryOperator.GreaterThanOrEqual => BinaryOperator.LessThanOrEqual,
			BinaryOperator.LessThanOrEqual => BinaryOperator.GreaterThanOrEqual,
			_ => op // Equal, NotEqual, And, Or are symmetric
		};
	}

	/// <summary>
	/// Converts a BinaryOperator enum value to its ComparisonOperator representation.
	/// </summary>
	private static ComparisonOperator GetComparisonOperator(BinaryOperator op)
	{
		return op switch
		{
			BinaryOperator.Equal => ComparisonOperator.Equals,
			BinaryOperator.NotEqual => ComparisonOperator.NotEquals,
			BinaryOperator.GreaterThan => ComparisonOperator.GreaterThan,
			BinaryOperator.LessThan => ComparisonOperator.LessThan,
			BinaryOperator.GreaterThanOrEqual => ComparisonOperator.GreaterThanOrEqual,
			BinaryOperator.LessThanOrEqual => ComparisonOperator.LessThanOrEqual,
			BinaryOperator.Between => ComparisonOperator.Between,
			_ => throw new NotSupportedException($"Operator '{op}' is not supported in WHERE conditions")
		};
	}

	/// <summary>
	/// Fallback method to parse ORDER BY and LIMIT clauses directly from the SQL string
	/// when the grammar parser fails to handle them.
	/// </summary>
	private void FallbackParseOrderByAndLimit(string query, ParsedQuery result) // todo: move to AST
	{
		// Handle ORDER BY
		if (query.Contains("ORDER BY", StringComparison.OrdinalIgnoreCase) && result.OrderBy == null)
		{
			var orderByIndex = query.IndexOf("ORDER BY", StringComparison.OrdinalIgnoreCase);
			var orderByClause = query.Substring(orderByIndex);

			// If there's a LIMIT after ORDER BY, trim it
			if (orderByClause.Contains("LIMIT", StringComparison.OrdinalIgnoreCase))
			{
				orderByClause = orderByClause.Substring(0, orderByClause.IndexOf("LIMIT", StringComparison.OrdinalIgnoreCase));
			}

			// Remove "ORDER BY" prefix
			orderByClause = orderByClause.Substring("ORDER BY".Length).Trim();

			// Parse each ORDER BY item
			var orderByItems = new List<OrderInfo>();
			foreach (var item in orderByClause.Split(','))
			{
				var trimmedItem = item.Trim();
				var direction = SortDirection.Ascending;
				var propertyPath = trimmedItem;

				// Check for DESC/ASC
				if (trimmedItem.EndsWith(" DESC", StringComparison.OrdinalIgnoreCase))
				{
					direction = SortDirection.Descending;
					propertyPath = trimmedItem.Substring(0, trimmedItem.Length - " DESC".Length).Trim();
				}
				else if (trimmedItem.EndsWith(" ASC", StringComparison.OrdinalIgnoreCase))
				{
					propertyPath = trimmedItem.Substring(0, trimmedItem.Length - " ASC".Length).Trim();
				}

				orderByItems.Add(new OrderInfo
				{
					PropertyPath = propertyPath,
					Direction = direction
				});
			}

			if (orderByItems.Count > 0)
			{
				result.OrderBy = orderByItems;
			}
		}

		// Handle LIMIT
		if (query.Contains("LIMIT", StringComparison.OrdinalIgnoreCase) && result.Limit == 0)
		{
			var limitIndex = query.IndexOf("LIMIT", StringComparison.OrdinalIgnoreCase);
			var limitClause = query.Substring(limitIndex + "LIMIT".Length).Trim();

			// Extract the limit number
			var limitValue = 0;
			foreach (var c in limitClause)
			{
				if (char.IsDigit(c))
				{
					limitValue = limitValue * 10 + (c - '0');
				}
				else
				{
					break;
				}
			}

			if (limitValue > 0)
			{
				result.Limit = limitValue;
			}
		}
	}
}
