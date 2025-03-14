// Executes queries on in-memory data

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using TimAbell.MockableCosmos.Parsing;

namespace TimAbell.MockableCosmos;

public class CosmosDbQueryExecutor
{
    private static ILogger _logger;

    public static void SetLogger(ILogger logger)
    {
        _logger = logger;
    }

    public static IEnumerable<JObject> Execute(ParsedQuery query, List<JObject> store)
    {
        if (_logger != null)
        {
            _logger.LogDebug("Executing query on {count} documents", store.Count);
            _logger.LogDebug("Query details - AST: {ast}", query.SprachedSqlAst != null ? "Present" : "null");

            if (query.WhereConditions != null)
            {
                _logger.LogDebug("WhereConditions count: {count}", query.WhereConditions.Count);
                foreach (var condition in query.WhereConditions)
                {
                    _logger.LogDebug("Condition: {property} {operator} {value}",
                        condition.PropertyPath, condition.Operator, condition.Value);
                }
            }
            else
            {
                _logger.LogDebug("No WhereConditions present in ParsedQuery");
            }
        }

        var filtered = store.AsQueryable();

        // Apply WHERE if specified
        if (query.SprachedSqlAst != null && query.SprachedSqlAst.Where != null)
        {
            if (_logger != null)
            {
                _logger.LogDebug("Applying WHERE from AST");
            }
            filtered = filtered.Where(e => ApplyWhere(e, query.SprachedSqlAst.Where.Condition));
        }
        else if (query.WhereConditions != null && query.WhereConditions.Count > 0)
        {
            if (_logger != null)
            {
                _logger.LogDebug("Applying WHERE from WhereConditions");
            }

            // Remove the statement body from the lambda expression
            filtered = filtered.Where(e => EvaluateWhereConditions(e, query.WhereConditions));

            // Log outside the expression tree if needed
            if (_logger != null && filtered.Any())
            {
                var firstItem = filtered.First();
                _logger.LogDebug("Evaluated document {id} against WHERE conditions: {result}",
                    firstItem["id"], EvaluateWhereConditions(firstItem, query.WhereConditions));
            }
        }
        else
        {
            if (_logger != null)
            {
                _logger.LogDebug("No WHERE conditions to apply");
            }
        }

        // Apply ORDER BY if specified
        if (query.SprachedSqlAst != null && query.SprachedSqlAst.OrderBy != null && query.SprachedSqlAst.OrderBy.Items.Count > 0)
        {
            if (_logger != null)
            {
                _logger.LogDebug("Applying ORDER BY from AST");
            }
            foreach (var orderByItem in query.SprachedSqlAst.OrderBy.Items)
            {
                if (_logger != null)
                {
                    _logger.LogDebug("ORDER BY {property} {direction}",
                        orderByItem.PropertyPath, orderByItem.Descending ? "DESC" : "ASC");
                }
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
        else if (query.OrderBy != null && query.OrderBy.Count > 0)
        {
            if (_logger != null)
            {
                _logger.LogDebug("Applying ORDER BY from ParsedQuery.OrderBy");
            }
            foreach (var orderBy in query.OrderBy)
            {
                if (_logger != null)
                {
                    _logger.LogDebug("ORDER BY {property} {direction}", orderBy.PropertyPath, orderBy.Direction);
                }
                if (orderBy.Direction.Equals("DESC", StringComparison.OrdinalIgnoreCase))
                {
                    filtered = filtered.OrderByDescending(e => GetPropertyValue(e, orderBy.PropertyPath));
                }
                else
                {
                    filtered = filtered.OrderBy(e => GetPropertyValue(e, orderBy.PropertyPath));
                }
            }
        }

        var results = filtered.ToList();
        if (_logger != null)
        {
            _logger.LogDebug("After filtering and ordering, got {count} results", results.Count);
        }

        // Apply LIMIT if specified
        if (query.SprachedSqlAst != null && query.SprachedSqlAst.Limit != null)
        {
            if (_logger != null)
            {
                _logger.LogDebug("Applying LIMIT {limit} from AST", query.SprachedSqlAst.Limit.Value);
            }
            results = results.Take(query.SprachedSqlAst.Limit.Value).ToList();
        }
        else if (query.Limit > 0)
        {
            if (_logger != null)
            {
                _logger.LogDebug("Applying LIMIT {limit} from ParsedQuery.Limit", query.Limit);
            }
            results = results.Take(query.Limit).ToList();
        }

        if (_logger != null)
        {
            _logger.LogDebug("After limit, final results count: {count}", results.Count);
        }

        // Apply SELECT projection if not SELECT *
        if (query.SprachedSqlAst != null && query.SprachedSqlAst.Select != null && !query.SprachedSqlAst.Select.IsSelectAll)
        {
            var properties = GetSelectedProperties(query.SprachedSqlAst.Select);
            if (_logger != null)
            {
                _logger.LogDebug("Applying projection from AST for properties: {properties}", string.Join(", ", properties));
            }
            return ApplyProjection(results, properties);
        }
        else if (query.PropertyPaths != null && query.PropertyPaths.Count > 0 && !query.PropertyPaths.Contains("*"))
        {
            if (_logger != null)
            {
                _logger.LogDebug("Applying projection for properties: {properties}", string.Join(", ", query.PropertyPaths));
            }
            return ApplyProjection(results, query.PropertyPaths);
        }

        return results;
    }

    // Helper method to evaluate WHERE conditions with detailed logging
    private static bool EvaluateWhereConditions(JObject document, List<WhereCondition> conditions)
    {
        if (conditions == null || conditions.Count == 0)
        {
            return true;
        }

        foreach (var condition in conditions)
        {
            var propertyValue = GetPropertyByPath(document, condition.PropertyPath);
            if (_logger != null)
            {
                _logger.LogDebug("Checking {property} {operator} {value}",
                    condition.PropertyPath, condition.Operator, condition.Value);
                _logger.LogDebug("Document property value: {value} (Type: {type})",
                    propertyValue?.ToString() ?? "null", propertyValue?.Type.ToString() ?? "null");
            }

            var matches = EvaluateCondition(propertyValue, condition.Operator, condition.Value);
            if (_logger != null)
            {
                _logger.LogDebug("Condition result: {result}", matches);
            }

            if (!matches)
            {
                return false;
            }
        }

        return true;
    }

    private static bool EvaluateCondition(JToken propertyValue, string operatorText, JToken conditionValue)
    {
        // Handle null property values
        if (propertyValue == null)
        {
            if (_logger != null)
            {
                _logger.LogDebug("Property value is null, condition fails");
            }
            return false;
        }

        // Convert the condition value to a comparable format
        object conditionObj = null;
        if (conditionValue != null)
        {
            try
            {
                // Extract primitive value from JToken
                switch (conditionValue.Type)
                {
                    case JTokenType.String:
                        conditionObj = conditionValue.Value<string>();
                        break;
                    case JTokenType.Integer:
                        conditionObj = conditionValue.Value<int>();
                        break;
                    case JTokenType.Float:
                        conditionObj = conditionValue.Value<double>();
                        break;
                    case JTokenType.Boolean:
                        conditionObj = conditionValue.Value<bool>();
                        break;
                    default:
                        conditionObj = conditionValue.ToString();
                        break;
                }
            }
            catch (Exception ex)
            {
                if (_logger != null)
                {
                    _logger.LogError(ex, "Error extracting condition value: {message}", ex.Message);
                }
                return false;
            }
        }

        if (_logger != null)
        {
            _logger.LogDebug("Comparing using operator: {operator}", operatorText);
            _logger.LogDebug("Condition value after extraction: {value} (Type: {type})",
                conditionObj, conditionObj?.GetType().Name ?? "null");
        }

        // Handle different operators
        switch (operatorText.ToUpperInvariant())
        {
            case "=":
                if (propertyValue.Type == JTokenType.String && conditionObj is string stringValue)
                {
                    var result = string.Equals(propertyValue.Value<string>(), stringValue, StringComparison.OrdinalIgnoreCase);
                    if (_logger != null)
                    {
                        _logger.LogDebug("String equality check: '{property}' = '{value}' => {result}",
                            propertyValue.Value<string>(), stringValue, result);
                    }
                    return result;
                }
                return JToken.DeepEquals(propertyValue, JToken.FromObject(conditionObj));

            case "!=":
                return !JToken.DeepEquals(propertyValue, JToken.FromObject(conditionObj));

            case ">":
                return CompareValues(propertyValue, conditionObj) > 0;

            case ">=":
                return CompareValues(propertyValue, conditionObj) >= 0;

            case "<":
                return CompareValues(propertyValue, conditionObj) < 0;

            case "<=":
                return CompareValues(propertyValue, conditionObj) <= 0;

            case "CONTAINS":
                if (propertyValue.Type == JTokenType.String && conditionObj is string containsValue)
                {
                    return propertyValue.Value<string>().IndexOf(containsValue, StringComparison.OrdinalIgnoreCase) >= 0;
                }
                return false;

            case "STARTSWITH":
                if (propertyValue.Type == JTokenType.String && conditionObj is string startsWithValue)
                {
                    return propertyValue.Value<string>().StartsWith(startsWithValue, StringComparison.OrdinalIgnoreCase);
                }
                return false;

            default:
                if (_logger != null)
                {
                    _logger.LogWarning("Unsupported operator: {operator}", operatorText);
                }
                return false;
        }
    }

    private static int CompareValues(JToken token, object value)
    {
        if (token == null || value == null)
        {
            return 0;
        }

        try
        {
            if (token.Type == JTokenType.Integer && value is int intValue)
            {
                return token.Value<int>().CompareTo(intValue);
            }
            else if (token.Type == JTokenType.Float && value is double doubleValue)
            {
                return token.Value<double>().CompareTo(doubleValue);
            }
            else if (token.Type == JTokenType.String && value is string stringValue)
            {
                return string.Compare(token.Value<string>(), stringValue, StringComparison.OrdinalIgnoreCase);
            }
            // Add more comparisons as needed
        }
        catch (Exception ex)
        {
            if (_logger != null)
            {
                _logger.LogError(ex, "Error comparing values: {message}", ex.Message);
            }
        }

        return 0;
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

    private static bool ApplyLegacyWhereConditions(JObject item, List<WhereCondition> whereConditions)
    {
        // All conditions must be true (AND semantics)
        foreach (var condition in whereConditions)
        {
            var propValue = GetPropertyValue(item, condition.PropertyPath);

            // For string comparisons, we might need to handle the stored JToken format 
            if (!CompareCondition(propValue, condition.Operator, condition.Value))
            {
                return false;
            }
        }

        return true;
    }

    private static bool CompareCondition(object propValue, string operatorStr, JToken conditionValue)
    {
        // Handle null values
        if (propValue == null)
        {
            return false;
        }

        // Extract the value from JToken if needed
        object value = conditionValue.Type == JTokenType.String
            ? conditionValue.Value<string>()
            : conditionValue.ToObject<object>();

        // For string comparisons where we're comparing different types
        if (propValue is string propString && value is string valueString)
        {
            switch (operatorStr.ToUpper())
            {
                case "=":
                    return string.Equals(propString, valueString, StringComparison.OrdinalIgnoreCase);
                case "!=":
                    return !string.Equals(propString, valueString, StringComparison.OrdinalIgnoreCase);
                case "CONTAINS":
                    return propString.IndexOf(valueString, StringComparison.OrdinalIgnoreCase) >= 0;
                case "STARTSWITH":
                    return propString.StartsWith(valueString, StringComparison.OrdinalIgnoreCase);
                default:
                    throw new NotImplementedException($"String operator {operatorStr} not implemented");
            }
        }

        // For numeric comparisons
        if (propValue is IComparable comparable && value != null)
        {
            int comparisonResult;
            try
            {
                // Try to convert types if needed
                if (propValue.GetType() != value.GetType())
                {
                    value = Convert.ChangeType(value, propValue.GetType());
                }
                comparisonResult = comparable.CompareTo(value);
            }
            catch
            {
                // If conversion fails, they're not comparable
                return false;
            }

            switch (operatorStr)
            {
                case "=":
                    return comparisonResult == 0;
                case "!=":
                    return comparisonResult != 0;
                case ">":
                    return comparisonResult > 0;
                case ">=":
                    return comparisonResult >= 0;
                case "<":
                    return comparisonResult < 0;
                case "<=":
                    return comparisonResult <= 0;
                default:
                    throw new NotImplementedException($"Comparison operator {operatorStr} not implemented");
            }
        }

        // Default equality check
        return propValue.Equals(value);
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