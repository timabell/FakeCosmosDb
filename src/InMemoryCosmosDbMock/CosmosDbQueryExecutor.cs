// Executes queries on in-memory data

using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace TimAbell.MockableCosmos;

public class CosmosDbQueryExecutor
{
    public static IEnumerable<JObject> Execute(ParsedQuery query, Dictionary<string, JObject> store)
    {
        var filtered = store.Values.AsQueryable();

        if (query.WhereClause != null)
        {
            filtered = filtered.Where(e => ApplyWhere(e, query.WhereClause));
        }

        if (query.OrderBy != null)
        {
            filtered = filtered.OrderBy(e => GetPropertyValue(e, query.OrderBy));
        }

        var results = filtered.ToList();

        // Apply LIMIT if specified
        if (query.Limit.HasValue)
        {
            results = results.Take(query.Limit.Value).ToList();
        }

        // Apply SELECT projection if not SELECT *
        if (!query.IsSelectAll)
        {
            return ApplyProjection(results, query.GetSelectedProperties());
        }

        return results;
    }

    private static IEnumerable<JObject> ApplyProjection(IEnumerable<JObject> results, IEnumerable<string> properties)
    {
        var projectedResults = new List<JObject>();
        var propertyPaths = properties.ToList();

        foreach (var item in results)
        {
            var projectedItem = new JObject();

            // Always include id property for consistency
            if (item.ContainsKey("id"))
            {
                projectedItem["id"] = item["id"];
            }

            // Add requested properties
            foreach (var propertyPath in propertyPaths)
            {
                string normalizedPath = propertyPath.Trim();

                // Handle alias references (c.Name -> Name, c.Address.City -> Address.City)
                if (normalizedPath.StartsWith("c."))
                {
                    normalizedPath = normalizedPath.Substring(2);
                }

                if (normalizedPath.Contains("."))
                {
                    // This is a nested property path like Address.City
                    var pathParts = normalizedPath.Split('.');
                    var rootProperty = pathParts[0];

                    // Get the source root property (e.g., Address)
                    var sourceProperty = item[rootProperty];
                    if (sourceProperty != null)
                    {
                        // If this is the first time we're adding this root property
                        if (!projectedItem.ContainsKey(rootProperty))
                        {
                            projectedItem[rootProperty] = new JObject();
                        }

                        // Navigate to the nested property (e.g., City)
                        var targetProperty = projectedItem[rootProperty] as JObject;
                        var currentSource = sourceProperty;

                        // For paths like Address.SubAddress.City
                        for (int i = 1; i < pathParts.Length - 1; i++)
                        {
                            var part = pathParts[i];
                            currentSource = currentSource[part];

                            if (currentSource == null)
                                break;

                            if (!targetProperty.ContainsKey(part))
                            {
                                targetProperty[part] = new JObject();
                            }

                            targetProperty = targetProperty[part] as JObject;
                        }

                        // Set the final property value if we still have a valid source
                        if (currentSource != null)
                        {
                            var finalPart = pathParts[pathParts.Length - 1];
                            var finalValue = currentSource[finalPart];

                            if (finalValue != null)
                            {
                                targetProperty[finalPart] = finalValue;
                            }
                        }
                    }
                }
                else
                {
                    // This is a simple property
                    var value = item[normalizedPath];
                    if (value != null)
                    {
                        projectedItem[normalizedPath] = value;
                    }
                }
            }

            projectedResults.Add(projectedItem);
        }

        return projectedResults;
    }

    private static bool ApplyWhere(JObject entity, string condition)
    {
        // Handle CONTAINS function
        if (condition.Contains("CONTAINS(", StringComparison.OrdinalIgnoreCase))
        {
            return HandleContainsFunction(entity, condition);
        }

        // Handle STARTSWITH function
        if (condition.Contains("STARTSWITH(", StringComparison.OrdinalIgnoreCase))
        {
            return HandleStartsWithFunction(entity, condition);
        }

        // Handle basic equality
        if (condition.Contains("="))
        {
            var parts = condition.Split('=');
            var field = parts[0].Trim();
            var value = parts[1].Trim().Trim('\'', '"');

            // Handle the 'c.' prefix in field paths
            if (field.StartsWith("c."))
            {
                field = field.Substring(2);
            }

            return string.Equals(
                GetPropertyValue(entity, field)?.ToString(),
                value,
                StringComparison.OrdinalIgnoreCase
            );
        }

        // Handle greater than
        if (condition.Contains(">"))
        {
            var parts = condition.Split('>');
            var field = parts[0].Trim();
            var value = parts[1].Trim().Trim('\'', '"');

            // Handle the 'c.' prefix in field paths
            if (field.StartsWith("c."))
            {
                field = field.Substring(2);
            }

            var propValue = GetPropertyValue(entity, field);
            if (propValue == null) return false;

            if (double.TryParse(value, out var numericValue) && double.TryParse(propValue.ToString(), out var propNumericValue))
            {
                return propNumericValue > numericValue;
            }

            return string.Compare(propValue.ToString(), value, StringComparison.OrdinalIgnoreCase) > 0;
        }

        // Handle less than
        if (condition.Contains("<"))
        {
            var parts = condition.Split('<');
            var field = parts[0].Trim();
            var value = parts[1].Trim().Trim('\'', '"');

            // Handle the 'c.' prefix in field paths
            if (field.StartsWith("c."))
            {
                field = field.Substring(2);
            }

            var propValue = GetPropertyValue(entity, field);
            if (propValue == null) return false;

            if (double.TryParse(value, out var numericValue) && double.TryParse(propValue.ToString(), out var propNumericValue))
            {
                return propNumericValue < numericValue;
            }

            return string.Compare(propValue.ToString(), value, StringComparison.OrdinalIgnoreCase) < 0;
        }

        return false;
    }

    private static bool HandleContainsFunction(JObject entity, string condition)
    {
        // Extract parameters from CONTAINS(field, value)
        var functionContent = ExtractFunctionContent(condition, "CONTAINS");
        var parameters = SplitFunctionParameters(functionContent);

        if (parameters.Length != 2) return false;

        var field = parameters[0].Trim();
        var searchValue = parameters[1].Trim().Trim('\'', '"');

        // Handle the 'c.' prefix in field paths
        if (field.StartsWith("c."))
        {
            field = field.Substring(2);
        }

        var propValue = GetPropertyValue(entity, field);
        if (propValue == null) return false;

        return propValue.ToString().IndexOf(searchValue, StringComparison.OrdinalIgnoreCase) >= 0;
    }

    private static bool HandleStartsWithFunction(JObject entity, string condition)
    {
        // Extract parameters from STARTSWITH(field, value)
        var functionContent = ExtractFunctionContent(condition, "STARTSWITH");
        var parameters = SplitFunctionParameters(functionContent);

        if (parameters.Length != 2) return false;

        var field = parameters[0].Trim();
        var searchValue = parameters[1].Trim().Trim('\'', '"');

        // Handle the 'c.' prefix in field paths
        if (field.StartsWith("c."))
        {
            field = field.Substring(2);
        }

        var propValue = GetPropertyValue(entity, field);
        if (propValue == null) return false;

        return propValue.ToString().StartsWith(searchValue, StringComparison.OrdinalIgnoreCase);
    }

    private static string ExtractFunctionContent(string condition, string functionName)
    {
        var startIndex = condition.IndexOf(functionName + "(", StringComparison.OrdinalIgnoreCase) + functionName.Length + 1;
        var endIndex = condition.IndexOf(')', startIndex);
        return condition.Substring(startIndex, endIndex - startIndex);
    }

    private static string[] SplitFunctionParameters(string parameters)
    {
        return parameters.Split(',');
    }

    private static object GetPropertyValue(JObject entity, string propertyPath)
    {
        if (string.IsNullOrEmpty(propertyPath))
            return null;

        // Handle 'c.' prefix
        if (propertyPath.StartsWith("c."))
        {
            propertyPath = propertyPath.Substring(2);
        }

        // Handle nested properties with dot notation (e.g., "address.city")
        var parts = propertyPath.Split('.');
        JToken current = entity;

        foreach (var part in parts)
        {
            if (current == null)
                return null;

            // Check if the property exists
            if (current.Type == JTokenType.Object && !((JObject)current).ContainsKey(part))
                return null;

            current = current[part];
        }

        return current;
    }
}