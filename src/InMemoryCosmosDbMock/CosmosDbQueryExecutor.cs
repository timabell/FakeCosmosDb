// Executes queries on in-memory data
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

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

        foreach (var item in results)
        {
            var projectedItem = new JObject();
            
            // Always include id property for consistency
            if (item.ContainsKey("id"))
            {
                projectedItem["id"] = item["id"];
            }
            
            // Add requested properties
            foreach (var property in properties)
            {
                var propertyPath = property.Trim();
                var value = GetPropertyValue(item, propertyPath);
                
                if (value != null)
                {
                    // Handle nested properties with dot notation
                    if (propertyPath.Contains("."))
                    {
                        var parts = propertyPath.Split('.');
                        var currentObj = projectedItem;
                        
                        for (int i = 0; i < parts.Length - 1; i++)
                        {
                            var part = parts[i];
                            if (!currentObj.ContainsKey(part))
                            {
                                currentObj[part] = new JObject();
                            }
                            currentObj = (JObject)currentObj[part];
                        }
                        
                        currentObj[parts[^1]] = JToken.FromObject(value);
                    }
                    else
                    {
                        projectedItem[propertyPath] = JToken.FromObject(value);
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
        if (condition.Contains("CONTAINS("))
        {
            return HandleContainsFunction(entity, condition);
        }
        
        // Handle STARTSWITH function
        if (condition.Contains("STARTSWITH("))
        {
            return HandleStartsWithFunction(entity, condition);
        }
        
        // Handle basic equality
        if (condition.Contains("="))
        {
            var parts = condition.Split('=');
            var field = parts[0].Trim();
            var value = parts[1].Trim().Trim('\'', '"');

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
        
        var propValue = GetPropertyValue(entity, field);
        return propValue?.ToString().IndexOf(searchValue, StringComparison.OrdinalIgnoreCase) >= 0;
    }
    
    private static bool HandleStartsWithFunction(JObject entity, string condition)
    {
        // Extract parameters from STARTSWITH(field, value)
        var functionContent = ExtractFunctionContent(condition, "STARTSWITH");
        var parameters = SplitFunctionParameters(functionContent);
        
        if (parameters.Length != 2) return false;
        
        var field = parameters[0].Trim();
        var searchValue = parameters[1].Trim().Trim('\'', '"');
        
        var propValue = GetPropertyValue(entity, field);
        return propValue?.ToString().StartsWith(searchValue, StringComparison.OrdinalIgnoreCase) ?? false;
    }
    
    private static string ExtractFunctionContent(string condition, string functionName)
    {
        var startIndex = condition.IndexOf(functionName + "(") + functionName.Length + 1;
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
            
        // Handle nested properties with dot notation (e.g., "address.city")
        var parts = propertyPath.Split('.');
        JToken current = entity;
        
        foreach (var part in parts)
        {
            current = current[part];
            if (current == null)
                return null;
        }
        
        return current;
    }
}
