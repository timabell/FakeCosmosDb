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
        var parsedQuery = CosmosDbSqlGrammar.ParseQuery(query);
        return ConvertToLegacyParsedQuery(parsedQuery);
    }

    /// <summary>
    /// Converts the new AST-based parsed query to the legacy ParsedQuery format
    /// for compatibility with existing code.
    /// </summary>
    private ParsedQuery ConvertToLegacyParsedQuery(CosmosDbSqlQuery query)
    {
        var result = new ParsedQuery
        {
            PropertyPaths = ExtractPropertyPaths(query.Select),
            FromName = query.From.Source,
            FromAlias = query.From.Alias,
            OrderBy = query.OrderBy != null ? ExtractOrderBy(query.OrderBy) : null,
            Limit = query.Limit?.Value ?? 0
        };

        if (query.Where != null)
        {
            result.WhereConditions = ExtractWhereConditions(query.Where.Condition);
        }

        return result;
    }

    /// <summary>
    /// Extracts property paths from the SELECT clause.
    /// </summary>
    private List<string> ExtractPropertyPaths(SelectClause select)
    {
        if (select.IsSelectAll)
        {
            return new List<string> { "*" };
        }

        return select.Items
            .OfType<PropertySelectItem>()
            .Select(item => item.PropertyPath)
            .ToList();
    }

    /// <summary>
    /// Extracts order by information from the ORDER BY clause.
    /// </summary>
    private List<OrderInfo> ExtractOrderBy(OrderByClause orderBy)
    {
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

        // If it's a binary AND expression, collect conditions from both sides
        if (condition is BinaryExpression binaryExpr && binaryExpr.Operator == BinaryOperator.And)
        {
            conditions.AddRange(ExtractWhereConditions(binaryExpr.Left));
            conditions.AddRange(ExtractWhereConditions(binaryExpr.Right));
        }
        // If it's a binary comparison (=, >, <, etc.), convert to a WhereCondition
        else if (condition is BinaryExpression comparison &&
                comparison.Operator != BinaryOperator.And &&
                comparison.Operator != BinaryOperator.Or)
        {
            if (comparison.Left is PropertyExpression prop && comparison.Right is ConstantExpression constant)
            {
                conditions.Add(new WhereCondition
                {
                    PropertyPath = prop.PropertyPath,
                    Operator = GetOperatorString(comparison.Operator),
                    Value = JToken.FromObject(constant.Value)
                });
            }
            else if (comparison.Right is PropertyExpression rightProp && comparison.Left is ConstantExpression leftConstant)
            {
                // Handle reverse order (value = property)
                conditions.Add(new WhereCondition
                {
                    PropertyPath = rightProp.PropertyPath,
                    Operator = GetOperatorString(comparison.Operator),
                    Value = JToken.FromObject(leftConstant.Value)
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

        return conditions;
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
}
