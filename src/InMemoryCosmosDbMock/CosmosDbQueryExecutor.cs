// Executes queries on in-memory data

using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using TimAbell.MockableCosmos.Parsing;

namespace TimAbell.MockableCosmos;

public class CosmosDbQueryExecutor
{
    public static IEnumerable<JObject> Execute(ParsedQuery query, List<JObject> store)
    {
        var filtered = store.AsQueryable();

        // Apply WHERE if specified
        if (query.SprachedSqlAst?.Where != null)
        {
            filtered = filtered.Where(e => ApplyWhere(e, query.SprachedSqlAst.Where.Condition));
        }

        // Apply ORDER BY if specified
        if (query.SprachedSqlAst?.OrderBy != null && query.SprachedSqlAst.OrderBy.Items.Count > 0)
        {
            foreach (var orderByItem in query.SprachedSqlAst.OrderBy.Items)
            {
                if (orderByItem.Descending)
                {
                    filtered = filtered.OrderByDescending(e => GetPropertyValue(e, orderByItem.PropertyPath));
                }
                else
                {
                    filtered = filtered.OrderBy(e => GetPropertyValue(e, orderByItem.PropertyPath));
                }
            }
        }

        var results = filtered.ToList();

        // Apply LIMIT if specified
        if (query.SprachedSqlAst?.Limit != null)
        {
            results = results.Take(query.SprachedSqlAst.Limit.Value).ToList();
        }

        // Apply SELECT projection if not SELECT *
        if (query.SprachedSqlAst?.Select != null && !query.SprachedSqlAst.Select.IsSelectAll)
        {
            return ApplyProjection(results, GetSelectedProperties(query.SprachedSqlAst.Select));
        }

        return results;
    }

    private static IEnumerable<string> GetSelectedProperties(SelectClause selectClause)
    {
        return selectClause.Items
            .OfType<PropertySelectItem>()
            .Select(item => item.PropertyPath)
            .ToList();
    }

    private static IEnumerable<JObject> ApplyProjection(IEnumerable<JObject> results, IEnumerable<string> properties)
    {
        var projectedResults = new List<JObject>();
        var propertyPaths = properties.ToList();

        foreach (var item in results)
        {
            var projectedItem = new JObject();

            foreach (var path in propertyPaths)
            {
                var propValue = GetPropertyByPath(item, path);
                if (propValue != null)
                {
                    SetPropertyByPath(projectedItem, path, propValue);
                }
            }

            projectedResults.Add(projectedItem);
        }

        return projectedResults;
    }

    private static object GetPropertyValue(JObject item, string propertyPath)
    {
        return GetPropertyByPath(item, propertyPath)?.Value<object>();
    }

    private static JToken GetPropertyByPath(JObject item, string path)
    {
        var parts = path.Split('.');
        JToken current = item;

        foreach (var part in parts)
        {
            if (current is JObject obj)
            {
                current = obj[part];
                if (current == null)
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
        }

        return current;
    }

    private static void SetPropertyByPath(JObject item, string path, JToken value)
    {
        var parts = path.Split('.');
        var current = item;

        // Navigate to the last parent in the path
        for (int i = 0; i < parts.Length - 1; i++)
        {
            var part = parts[i];
            if (current[part] == null || !(current[part] is JObject))
            {
                current[part] = new JObject();
            }
            current = (JObject)current[part];
        }

        // Set the value on the last part
        current[parts[parts.Length - 1]] = value;
    }

    private static bool ApplyWhere(JObject item, Expression condition)
    {
        return EvaluateExpression(item, condition);
    }

    private static bool EvaluateExpression(JObject item, Expression expression)
    {
        if (expression is BinaryExpression binary)
        {
            var leftValue = EvaluateValue(item, binary.Left);
            var rightValue = EvaluateValue(item, binary.Right);

            switch (binary.Operator)
            {
                case BinaryOperator.Equal:
                    return Equals(leftValue, rightValue);
                case BinaryOperator.NotEqual:
                    return !Equals(leftValue, rightValue);
                case BinaryOperator.GreaterThan:
                    return CompareValues(leftValue, rightValue) > 0;
                case BinaryOperator.LessThan:
                    return CompareValues(leftValue, rightValue) < 0;
                case BinaryOperator.GreaterThanOrEqual:
                    return CompareValues(leftValue, rightValue) >= 0;
                case BinaryOperator.LessThanOrEqual:
                    return CompareValues(leftValue, rightValue) <= 0;
                case BinaryOperator.And:
                    return EvaluateExpression(item, binary.Left) && EvaluateExpression(item, binary.Right);
                case BinaryOperator.Or:
                    return EvaluateExpression(item, binary.Left) || EvaluateExpression(item, binary.Right);
                default:
                    throw new NotImplementedException($"Operator {binary.Operator} not implemented");
            }
        }

        if (expression is PropertyExpression prop)
        {
            // For boolean property expressions, simply check if the property exists and is true
            var value = GetPropertyValue(item, prop.PropertyPath);
            if (value is bool boolValue)
            {
                return boolValue;
            }
            return value != null;
        }

        if (expression is FunctionCallExpression func)
        {
            return EvaluateFunction(item, func);
        }

        throw new NotImplementedException($"Expression type {expression.GetType().Name} not implemented");
    }

    private static object EvaluateValue(JObject item, Expression expression)
    {
        if (expression is ConstantExpression constant)
        {
            return constant.Value;
        }

        if (expression is PropertyExpression prop)
        {
            return GetPropertyValue(item, prop.PropertyPath);
        }

        if (expression is FunctionCallExpression func)
        {
            return EvaluateFunction(item, func);
        }

        throw new NotImplementedException($"Value expression type {expression.GetType().Name} not implemented");
    }

    private static bool EvaluateFunction(JObject item, FunctionCallExpression function)
    {
        if (string.Equals(function.Name, "CONTAINS", StringComparison.OrdinalIgnoreCase) && function.Arguments.Count == 2)
        {
            var propertyValue = EvaluateValue(item, function.Arguments[0])?.ToString();
            var searchValue = EvaluateValue(item, function.Arguments[1])?.ToString();

            if (propertyValue == null || searchValue == null)
            {
                return false;
            }

            return propertyValue.Contains(searchValue);
        }

        if (string.Equals(function.Name, "STARTSWITH", StringComparison.OrdinalIgnoreCase) && function.Arguments.Count == 2)
        {
            var propertyValue = EvaluateValue(item, function.Arguments[0])?.ToString();
            var searchValue = EvaluateValue(item, function.Arguments[1])?.ToString();

            if (propertyValue == null || searchValue == null)
            {
                return false;
            }

            return propertyValue.StartsWith(searchValue);
        }

        throw new NotImplementedException($"Function {function.Name} not implemented");
    }

    private static int CompareValues(object left, object right)
    {
        if (left == null && right == null)
        {
            return 0;
        }

        if (left == null)
        {
            return -1;
        }

        if (right == null)
        {
            return 1;
        }

        if (left is IComparable comparable)
        {
            try
            {
                return comparable.CompareTo(right);
            }
            catch
            {
                // Fall back to string comparison if direct comparison fails
                return comparable.ToString().CompareTo(right.ToString());
            }
        }

        // Default to string comparison
        return left.ToString().CompareTo(right.ToString());
    }
}