using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace TimAbell.MockableCosmos.Parsing;

/// <summary>
/// Implementation of ICosmosDbQueryParser that uses the Sprache parser combinator library.
/// </summary>
public class SpracheSqlQueryParser : ICosmosDbQueryParser
{
    /// <summary>
    /// Parses a CosmosDB SQL query using the Sprache grammar.
    /// </summary>
    public ParsedQuery Parse(string query)
    {
        try
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
        catch (Exception ex)
        {
            throw new FormatException($"Failed to parse CosmosDB SQL query: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Converts the new AST-based parsed query to the legacy ParsedQuery format
    /// for compatibility with existing code.
    /// </summary>
    private ParsedQuery ConvertToLegacyParsedQuery(CosmosDbSqlQuery query)
    {
        var result = new ParsedQuery
        {
            SprachedSqlAst = query,
            PropertyPaths = ExtractPropertyPaths(query.Select),
            FromName = query.From.Source,
            FromAlias = query.From.Alias
        };

        // Extract WHERE conditions
        if (query.Where != null)
        {
            var whereConditions = ExtractWhereConditions(query.Where.Condition);
            if (whereConditions.Count > 0)
            {
                result.WhereConditions = whereConditions;
            }
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
                Direction = item.Descending ? "DESC" : "ASC"
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
        // If it's a binary comparison (=, >, <, etc.), convert to a WhereCondition
        else if (condition is BinaryExpression comparison &&
                comparison.Operator != BinaryOperator.And &&
                comparison.Operator != BinaryOperator.Or)
        {
            if (comparison.Left is PropertyExpression leftProp && comparison.Right is ConstantExpression rightConst)
            {
                conditions.Add(new WhereCondition
                {
                    PropertyPath = leftProp.PropertyPath,
                    Operator = GetOperatorString(comparison.Operator),
                    Value = JToken.FromObject(rightConst.Value)
                });
            }
            else if (comparison.Right is PropertyExpression rightProp && comparison.Left is ConstantExpression leftConst)
            {
                // Handle reverse order (value = property)
                var reversedOp = GetReversedOperator(comparison.Operator);
                conditions.Add(new WhereCondition
                {
                    PropertyPath = rightProp.PropertyPath,
                    Operator = GetOperatorString(reversedOp),
                    Value = JToken.FromObject(leftConst.Value)
                });
            }
        }
        // If it's a function call like CONTAINS or STARTSWITH
        else if (condition is FunctionCallExpression functionCall)
        {
            // Extract function name
            string functionName = functionCall.FunctionName.ToUpperInvariant();

            if ((functionName == "CONTAINS" || functionName == "STARTSWITH") &&
                functionCall.Arguments.Count == 2 &&
                functionCall.Arguments[0] is PropertyExpression propExpr &&
                functionCall.Arguments[1] is ConstantExpression constExpr)
            {
                conditions.Add(new WhereCondition
                {
                    PropertyPath = propExpr.PropertyPath,
                    Operator = functionName,
                    Value = JToken.FromObject(constExpr.Value)
                });
            }
        }
        // If it's just a property expression (rare, but can happen)
        else if (condition is PropertyExpression propExpr)
        {
            conditions.Add(new WhereCondition
            {
                PropertyPath = propExpr.PropertyPath,
                Operator = "=",
                Value = JToken.FromObject(true)  // Treat as a boolean condition (property = true)
            });
        }

        return conditions;
    }

    /// <summary>
    /// Reverses a binary operator for reversed operand order
    /// </summary>
    private BinaryOperator GetReversedOperator(BinaryOperator op)
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
    /// Converts a BinaryOperator enum value to its string representation.
    /// </summary>
    private string GetOperatorString(BinaryOperator op)
    {
        return op switch
        {
            BinaryOperator.Equal => "=",
            BinaryOperator.NotEqual => "!=",
            BinaryOperator.GreaterThan => ">",
            BinaryOperator.LessThan => "<",
            BinaryOperator.GreaterThanOrEqual => ">=",
            BinaryOperator.LessThanOrEqual => "<=",
            _ => throw new ArgumentException($"Unsupported operator: {op}")
        };
    }

    /// <summary>
    /// Fallback method to parse ORDER BY and LIMIT clauses directly from the SQL string
    /// when the grammar parser fails to handle them.
    /// </summary>
    private void FallbackParseOrderByAndLimit(string query, ParsedQuery result)
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
                var direction = "ASC";
                var propertyPath = trimmedItem;

                // Check for DESC/ASC
                if (trimmedItem.EndsWith(" DESC", StringComparison.OrdinalIgnoreCase))
                {
                    direction = "DESC";
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
