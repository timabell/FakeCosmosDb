// Executes queries on in-memory data

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using TimAbell.FakeCosmosDb.SqlParser;

namespace TimAbell.FakeCosmosDb.Implementation;

public class CosmosDbQueryExecutor
{
	private readonly ILogger _logger;

	public CosmosDbQueryExecutor(ILogger logger = null)
	{
		_logger = logger;
	}

	public IEnumerable<JObject> Execute(ParsedQuery query, List<JObject> store, IReadOnlyList<(string Name, object Value)> parameters)
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

			filtered = filtered.Where(e => ApplyWhere(e, query.SprachedSqlAst.Where.Condition, parameters));
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

				if (orderBy.Direction == SortDirection.Descending)
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

		// Apply TOP if specified
		if (query.SprachedSqlAst?.Select?.Top != null)
		{
			if (_logger != null)
			{
				_logger.LogDebug("Applying TOP {top} from AST", query.SprachedSqlAst.Select.Top.Value);
			}

			results = results.Take(query.SprachedSqlAst.Select.Top.Value).ToList();
		}
		else if (query.TopValue > 0)
		{
			if (_logger != null)
			{
				_logger.LogDebug("Applying TOP {top} from ParsedQuery.TopValue", query.TopValue);
			}

			results = results.Take(query.TopValue).ToList();
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
	private bool EvaluateWhereConditions(JObject document, List<WhereCondition> conditions)
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
					propertyValue?.ToString() ?? "null", propertyValue != null ? propertyValue.Type.ToString() : "null");
			}

			var matches = EvaluateCondition(propertyValue, condition.Operator, condition.Value, condition.IgnoreCase ?? false);
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

	private bool EvaluateCondition(JToken propertyValue, ComparisonOperator op, JToken conditionValue, bool ignoreCase)
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

			if (_logger != null)
			{
				_logger.LogDebug("Extracted condition value: {value} (Type: {type})",
					conditionObj, conditionObj?.GetType().Name ?? "null");
				_logger.LogDebug("Property value for comparison: {value} (Type: {type})",
					propertyValue.ToString(), propertyValue.Type.ToString());
			}
		}

		if (_logger != null)
		{
			_logger.LogDebug("Comparing using operator: {operator}", op);
			_logger.LogDebug("Condition value after extraction: {value} (Type: {type})",
				conditionObj, conditionObj?.GetType().Name ?? "null");
		}

		// Handle different operators
		switch (op)
		{
			case ComparisonOperator.Equals:
				if (propertyValue.Type == JTokenType.String && conditionObj is string stringValue)
				{
					string propStringValue = propertyValue.Value<string>();
					var result = string.Equals(propStringValue, stringValue, StringComparison.OrdinalIgnoreCase);
					if (_logger != null)
					{
						_logger.LogDebug("String equality check: '{property}' = '{value}' => {result}",
							propStringValue, stringValue, result);
					}

					return result;
				}

				var equality = JToken.DeepEquals(propertyValue, JToken.FromObject(conditionObj));
				if (_logger != null)
				{
					_logger.LogDebug("Deep equality check: '{property}' = '{value}' => {result}",
						propertyValue.ToString(), conditionObj?.ToString() ?? "null", equality);
				}

				return equality;

			case ComparisonOperator.NotEquals:
				if (conditionObj is JToken jToken)
				{
					return !JToken.DeepEquals(propertyValue, jToken);
				}
				return !JToken.DeepEquals(propertyValue, JToken.FromObject(conditionObj));

			case ComparisonOperator.GreaterThan:
				return CompareValues(propertyValue, conditionObj) > 0;

			case ComparisonOperator.GreaterThanOrEqual:
				return CompareValues(propertyValue, conditionObj) >= 0;

			case ComparisonOperator.LessThan:
				return CompareValues(propertyValue, conditionObj) < 0;

			case ComparisonOperator.LessThanOrEqual:
				return CompareValues(propertyValue, conditionObj) <= 0;

			case ComparisonOperator.StringContains:
				var containsPropertyValue = propertyValue?.ToString();
				var containsSearchValue = conditionObj?.ToString();

				if (containsPropertyValue == null || containsSearchValue == null)
				{
					_logger?.LogDebug("String contains comparison: one of the values is null");
					return false;
				}

				var containsResult = ignoreCase
					? containsPropertyValue.IndexOf(containsSearchValue, StringComparison.OrdinalIgnoreCase) >= 0
					: containsPropertyValue.Contains(containsSearchValue);

				if (_logger != null)
				{
					_logger.LogDebug("String contains comparison: '{left}' contains '{right}' (ignoreCase: {ignoreCase}) = {result}",
						containsPropertyValue, containsSearchValue, ignoreCase, containsResult);
				}

				return containsResult;

			case ComparisonOperator.StringStartsWith:
				if (propertyValue.Type == JTokenType.String && conditionObj is string startsWithValue)
				{
					return propertyValue.Value<string>().StartsWith(startsWithValue, StringComparison.OrdinalIgnoreCase);
				}

				return false;

			case ComparisonOperator.IsDefined:
				return propertyValue != null;

			case ComparisonOperator.ArrayContains:
				if (propertyValue is JArray array)
				{
					// Extract the value to search for
					object searchValue = conditionObj;

					// If the search value is null, we can't meaningfully search for it
					if (searchValue == null)
					{
						return false;
					}

					// Convert the search value to string for comparison
					string searchString = searchValue.ToString();

					// Check each element in the array
					foreach (var element in array)
					{
						if (element != null && element.ToString() == searchString)
						{
							return true;
						}
					}

					return false;
				}

				// If propValue is not an array, return false
				return false;

			case ComparisonOperator.Between:
				if (conditionObj is not JArray arrayBetween)
				{
					throw new InvalidOperationException("BETWEEN operator requires an array of values");
				}

				if (arrayBetween.Count != 2)
				{
					throw new InvalidOperationException($"BETWEEN operator requires exactly 2 values, but found {arrayBetween.Count}");
				}

				// Extract the values to compare against
				var lowerBound = arrayBetween[0];
				var upperBound = arrayBetween[1];

				// Get the property value as a JToken if it's not already
				JToken jPropertyValue = propertyValue as JToken ?? JToken.FromObject(propertyValue);

				// Handle different types of JToken values
				if (jPropertyValue.Type == JTokenType.Integer)
				{
					int value = jPropertyValue.Value<int>();
					return value >= lowerBound.Value<int>() && value <= upperBound.Value<int>();
				}
				else if (jPropertyValue.Type == JTokenType.Float)
				{
					double value = jPropertyValue.Value<double>();
					return value >= lowerBound.Value<double>() && value <= upperBound.Value<double>();
				}
				else if (jPropertyValue.Type == JTokenType.Date)
				{
					DateTime value = jPropertyValue.Value<DateTime>();
					return value >= lowerBound.Value<DateTime>() && value <= upperBound.Value<DateTime>();
				}
				else if (jPropertyValue.Type == JTokenType.String)
				{
					string value = jPropertyValue.Value<string>();
					string lowerStr = lowerBound.ToString();
					string upperStr = upperBound.ToString();
					return string.Compare(value, lowerStr) >= 0 && string.Compare(value, upperStr) <= 0;
				}
				else
				{
					// For other types, try converting to string and compare
					string value = jPropertyValue.ToString();
					string lowerStr = lowerBound.ToString();
					string upperStr = upperBound.ToString();
					return string.Compare(value, lowerStr) >= 0 && string.Compare(value, upperStr) <= 0;
				}

			default:
				if (_logger != null)
				{
					_logger.LogWarning("Unsupported operator: {operator}", op);
				}

				return false;
		}
	}

	private int CompareValues(JToken token, object value)
	{
		if (token == null || value == null)
		{
			return 0;
		}

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

		return 0;
	}

	private int CompareValues(object left, object right)
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

		// Handle JValue objects
		if (left is JValue leftJValue)
		{
			left = leftJValue.Value;
		}

		if (right is JValue rightJValue)
		{
			right = rightJValue.Value;
		}

		// Handle numeric comparisons
		if (IsNumeric(left) && IsNumeric(right))
		{
			// Convert both to double for numeric comparison
			double leftDouble = Convert.ToDouble(left);
			double rightDouble = Convert.ToDouble(right);

			if (_logger != null)
			{
				_logger.LogDebug("Numeric comparison: {left} ({leftType}) vs {right} ({rightType})",
					left, left.GetType().Name, right, right.GetType().Name);
			}

			return leftDouble.CompareTo(rightDouble);
		}

		if (left is IComparable comparable && left.GetType() == right.GetType())
		{
			return comparable.CompareTo(right);
		}

		// Default to string comparison
		return left.ToString().CompareTo(right.ToString());
	}

	private bool IsNumeric(object value)
	{
		return value is sbyte || value is byte || value is short || value is ushort ||
			   value is int || value is uint || value is long || value is ulong ||
			   value is float || value is double || value is decimal;
	}

	private int CompareString(string left, string right, BinaryOperator operatorEnum)
	{
		int comparison = string.Compare(left, right, StringComparison.OrdinalIgnoreCase);

		switch (operatorEnum)
		{
			case BinaryOperator.Equal:
				return comparison == 0 ? 1 : 0;
			case BinaryOperator.NotEqual:
				return comparison != 0 ? 1 : 0;
			case BinaryOperator.GreaterThan:
				return comparison > 0 ? 1 : 0;
			case BinaryOperator.GreaterThanOrEqual:
				return comparison >= 0 ? 1 : 0;
			case BinaryOperator.LessThan:
				return comparison < 0 ? 1 : 0;
			case BinaryOperator.LessThanOrEqual:
				return comparison <= 0 ? 1 : 0;
			default:
				throw new NotImplementedException($"Operator {operatorEnum} not implemented for string comparison");
		}
	}

	private IEnumerable<string> GetSelectedProperties(SelectClause selectClause)
	{
		return selectClause.Items
			.OfType<PropertySelectItem>()
			.Select(item => item.PropertyPath)
			.ToList();
	}

	private IEnumerable<JObject> ApplyProjection(IEnumerable<JObject> results, IEnumerable<string> properties)
	{
		var projectedResults = new List<JObject>();
		var propertyPaths = properties.ToList();

		foreach (var item in results)
		{
			var projectedItem = new JObject();

			// Always include the 'id' field if it exists in the original document
			if (item["id"] != null)
			{
				projectedItem["id"] = item["id"];
			}

			foreach (var path in propertyPaths)
			{
				// Remove the FROM alias (like 'c.') if present at the beginning of the path
				string processedPath = path;
				if (path.Contains('.') && (path.StartsWith("c.") || path.StartsWith("r.")))
				{
					// Skip the alias (e.g., "c.") part
					processedPath = path.Substring(path.IndexOf('.') + 1);
				}

				var propValue = GetPropertyByPath(item, path);
				if (propValue != null)
				{
					SetPropertyByPath(projectedItem, processedPath, propValue);
				}
			}

			projectedResults.Add(projectedItem);
		}

		return projectedResults;
	}

	private object GetPropertyValue(JObject item, string propertyPath)
	{
		var token = GetPropertyByPath(item, propertyPath);
		object value = token?.Value<object>();

		if (_logger != null)
		{
			_logger.LogDebug("GetPropertyValue for path '{path}' returned: {value} (Type: {type})",
				propertyPath, value?.ToString() ?? "null", value?.GetType().Name ?? "null");
		}

		return value;
	}

	private JToken GetPropertyByPath(JObject item, string path)
	{
		if (string.IsNullOrEmpty(path))
		{
			return null;
		}

		// Special case for * to return the entire object
		if (path == "*")
		{
			return item;
		}

		var parts = path.Split('.');
		JToken current = item;

		// Skip the first part if it's the FROM alias
		int startIndex = 0;
		if (parts.Length > 1 && (parts[0] == "c" || parts[0] == "r")) // Common FROM aliases are 'c' and 'r'
		{
			startIndex = 1;
			if (_logger != null)
			{
				_logger.LogDebug("Skipping FROM alias '{alias}' in property path", parts[0]);
			}
		}

		// Navigate through the path parts
		for (int i = startIndex; i < parts.Length; i++)
		{
			if (current == null)
			{
				return null;
			}

			if (current is JObject obj)
			{
				// Case-insensitive property lookup
				string currentPart = parts[i];

				// First try direct lookup (faster)
				JToken directResult = obj[currentPart];
				if (directResult != null)
				{
					current = directResult;
					continue;
				}

				// If not found, try case-insensitive lookup
				var property = obj.Properties()
					.FirstOrDefault(p => string.Equals(p.Name, currentPart, StringComparison.OrdinalIgnoreCase));

				if (property != null)
				{
					current = property.Value;
					if (_logger != null)
					{
						_logger.LogDebug("Case-insensitive match found for '{requestedName}' -> '{actualName}'",
							currentPart, property.Name);
					}
				}
				else
				{
					// Property not found
					if (_logger != null)
					{
						_logger.LogDebug("Property '{name}' not found in object", currentPart);
					}
					return null;
				}
			}
			else
			{
				// Can't navigate further
				return null;
			}
		}

		return current;
	}

	private void SetPropertyByPath(JObject item, string path, JToken value)
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

	private bool ApplyWhere(JObject item, Expression condition, IReadOnlyList<(string Name, object Value)> parameters)
	{
		if (condition == null) return true;

		var result = EvaluateExpression(item, condition, parameters);

		// Convert the result to a boolean if it's not already
		bool boolResult;
		if (result is bool b)
		{
			boolResult = b;
		}
		else if (result is object) // Changed from result != null to resolve warning
		{
			boolResult = Convert.ToBoolean(result);
		}
		else
		{
			// Null evaluates to false
			boolResult = false;
		}

		if (_logger != null)
		{
			_logger.LogDebug("ApplyWhere result: {result} for item with id: {id}", boolResult, item["id"]);
		}

		return boolResult;
	}

	private bool ApplyLegacyWhereConditions(JObject item, List<WhereCondition> whereConditions)
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

	private bool CompareCondition(object propValue, ComparisonOperator operatorEnum, JToken conditionValue)
	{
		// Handle null values
		if (propValue == null)
		{
			// Special case for IS_NULL check (represented as Equals with null value)
			if (operatorEnum == ComparisonOperator.Equals && conditionValue.Type == JTokenType.Null)
			{
				return true;
			}

			// For IsDefined operator, propValue being null means the property is not defined
			if (operatorEnum == ComparisonOperator.IsDefined)
			{
				return false;
			}

			return false;
		}

		// For IsDefined operator, if we get here, the property exists
		if (operatorEnum == ComparisonOperator.IsDefined)
		{
			return true;
		}

		// For ArrayContains operator
		if (operatorEnum == ComparisonOperator.ArrayContains)
		{
			if (propValue is JArray array)
			{
				// Extract the value to search for
				object searchValue = conditionValue.ToObject<object>();

				// If the search value is null, we can't meaningfully search for it
				if (searchValue == null)
				{
					return false;
				}

				// Convert the search value to string for comparison
				string searchString = searchValue.ToString();

				// Check each element in the array
				foreach (var element in array)
				{
					if (element != null && element.ToString() == searchString)
					{
						return true;
					}
				}

				return false;
			}

			// If propValue is not an array, return false
			return false;
		}

		// Extract the value from JToken if needed
		object value = conditionValue.Type == JTokenType.String
			? conditionValue.Value<string>()
			: conditionValue.ToObject<object>();

		// For string comparisons where we're comparing different types
		if (propValue is string propString && value is string valueString)
		{
			switch (operatorEnum)
			{
				case ComparisonOperator.Equals:
					return string.Equals(propString, valueString, StringComparison.Ordinal);
				case ComparisonOperator.NotEquals:
					return !string.Equals(propString, valueString, StringComparison.Ordinal);
				case ComparisonOperator.StringContains:
					bool ignoreCase = false;
					if (conditionValue.Type == JTokenType.Boolean)
					{
						ignoreCase = conditionValue.Value<bool>();
					}
					return propString.Contains(valueString, ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
				case ComparisonOperator.StringStartsWith:
					return propString.StartsWith(valueString, StringComparison.Ordinal);
				default:
					// For other string comparisons, use string.Compare
					int comparison = string.Compare(propString, valueString, StringComparison.Ordinal);
					return GetComparisonResult(comparison, operatorEnum);
			}
		}

		// For numeric comparisons
		if (propValue is IComparable comparable && value != null)
		{
			// Convert value to the same type as propValue if possible
			if (propValue is int)
			{
				value = Convert.ToInt32(value);
			}
			else if (propValue is double)
			{
				value = Convert.ToDouble(value);
			}
			else if (propValue is decimal)
			{
				value = Convert.ToDecimal(value);
			}
			else if (propValue is long)
			{
				value = Convert.ToInt64(value);
			}

			int comparison = comparable.CompareTo(value);
			return GetComparisonResult(comparison, operatorEnum);
		}

		// For boolean values
		if (propValue is bool propBool && value is bool valueBool)
		{
			switch (operatorEnum)
			{
				case ComparisonOperator.Equals:
					return propBool == valueBool;
				case ComparisonOperator.NotEquals:
					return propBool != valueBool;
				default:
					return false; // Other comparisons don't make sense for booleans
			}
		}

		// If we can't compare properly, default to equality check
		return propValue.Equals(value);
	}

	private bool GetComparisonResult(int comparison, ComparisonOperator operatorEnum)
	{
		switch (operatorEnum)
		{
			case ComparisonOperator.Equals:
				return comparison == 0;
			case ComparisonOperator.NotEquals:
				return comparison != 0;
			case ComparisonOperator.GreaterThan:
				return comparison > 0;
			case ComparisonOperator.GreaterThanOrEqual:
				return comparison >= 0;
			case ComparisonOperator.LessThan:
				return comparison < 0;
			case ComparisonOperator.LessThanOrEqual:
				return comparison <= 0;
			default:
				throw new NotImplementedException($"Operator {operatorEnum} not implemented for comparison");
		}
	}

	private bool CompareValues(object propValue, object value, ComparisonOperator operatorEnum)
	{
		if (_logger != null)
		{
			_logger.LogDebug("Comparing values: '{left}' ({leftType}) {op} '{right}' ({rightType})",
				propValue?.ToString() ?? "null",
				propValue?.GetType().Name ?? "null",
				operatorEnum.ToString(),
				value?.ToString() ?? "null",
				value?.GetType().Name ?? "null");
		}

		// For null values
		if (propValue == null)
		{
			if (operatorEnum == ComparisonOperator.Equals)
			{
				return value == null;
			}
			else if (operatorEnum == ComparisonOperator.NotEquals)
			{
				return value != null;
			}

			// All other comparisons with null return false
			return false;
		}

		if (value == null)
		{
			if (operatorEnum == ComparisonOperator.NotEquals)
			{
				return true;
			}

			// All other comparisons with null return false
			return false;
		}

		// For string values
		if (propValue is string strPropValue && value is string strValue)
		{
			int comparison = string.Compare(strPropValue, strValue, StringComparison.Ordinal);
			return GetComparisonResult(comparison, operatorEnum);
		}

		// For numeric comparisons
		if (propValue is IComparable comparable && value != null)
		{
			// Convert value to the same type as propValue if possible
			if (propValue is int)
			{
				value = Convert.ToInt32(value);
			}
			else if (propValue is double)
			{
				value = Convert.ToDouble(value);
			}
			else if (propValue is decimal)
			{
				value = Convert.ToDecimal(value);
			}
			else if (propValue is long)
			{
				value = Convert.ToInt64(value);
			}

			int comparison = comparable.CompareTo(value);
			return GetComparisonResult(comparison, operatorEnum);
		}

		// For boolean values
		if (propValue is bool propBool && value is bool valueBool)
		{
			switch (operatorEnum)
			{
				case ComparisonOperator.Equals:
					return propBool == valueBool;
				case ComparisonOperator.NotEquals:
					return propBool != valueBool;
				default:
					return false; // Other comparisons don't make sense for booleans
			}
		}

		// Default case: convert to string and compare
		return CompareStringValues(propValue.ToString(), value.ToString(), operatorEnum);
	}

	private bool CompareStringValues(string left, string right, ComparisonOperator operatorEnum)
	{
		int comparison = string.Compare(left, right, StringComparison.Ordinal);
		return GetComparisonResult(comparison, operatorEnum);
	}

	private bool EvaluateExpression(JObject item, Expression expression, IReadOnlyList<(string Name, object Value)> parameters)
	{
		if (_logger != null)
		{
			_logger.LogDebug("Evaluating expression of type: {type}", expression.GetType().Name);
		}

		if (expression is BinaryExpression binary)
		{
			var leftValue = EvaluateValue(item, binary.Left, parameters);
			var rightValue = EvaluateValue(item, binary.Right, parameters);

			if (_logger != null)
			{
				_logger.LogDebug("Comparing values: '{left}' ({leftType}) {op} '{right}' ({rightType})",
					leftValue, leftValue?.GetType().Name,
					binary.Operator,
					rightValue, rightValue?.GetType().Name);
			}

			switch (binary.Operator)
			{
				case BinaryOperator.Equal:
					// Handle string comparison for JValue objects
					if (leftValue is JValue leftJValue && leftJValue.Type == JTokenType.String)
					{
						string leftStr = leftJValue.Value<string>();
						string rightStr = rightValue is JValue rightJValue && rightJValue.Type == JTokenType.String
							? rightJValue.Value<string>()
							: rightValue as string;

						if (rightStr != null)
						{
							if (_logger != null)
							{
								_logger.LogDebug("JValue string comparison: '{left}' = '{right}'", leftStr, rightStr);
							}
							return string.Equals(leftStr, rightStr, StringComparison.Ordinal);
						}
					}

					// Handle numeric comparisons
					else if ((leftValue is JValue leftNumJValue &&
							 (leftNumJValue.Type == JTokenType.Integer || leftNumJValue.Type == JTokenType.Float)) ||
							 IsNumeric(leftValue))
					{
						// Extract the numeric value from JValue if needed
						double leftNum;
						if (leftValue is JValue leftJNum)
						{
							leftNum = leftJNum.Value<double>();
						}
						else
						{
							leftNum = Convert.ToDouble(leftValue);
						}

						// Extract the numeric value from right side
						double rightNum;
						if (rightValue is JValue rightJNum &&
							(rightJNum.Type == JTokenType.Integer || rightJNum.Type == JTokenType.Float))
						{
							rightNum = rightJNum.Value<double>();
						}
						else if (IsNumeric(rightValue))
						{
							rightNum = Convert.ToDouble(rightValue);
						}
						else if (double.TryParse(rightValue?.ToString(), out double parsedNum))
						{
							rightNum = parsedNum;
						}
						else
						{
							// If right value is not numeric, we can't compare
							return false;
						}

						if (_logger != null)
						{
							_logger.LogDebug("Numeric comparison: {left} = {right}", leftNum, rightNum);
						}

						return Math.Abs(leftNum - rightNum) < 0.000001; // Use small epsilon for floating point comparison
					}
					else if (leftValue is JValue leftBoolJValue && leftBoolJValue.Type == JTokenType.Boolean)
					{
						var leftBool = leftBoolJValue.Value<bool>();
						if (_logger != null)
						{
							_logger.LogDebug("JValue boolean comparison: {left} = {right}", leftBool, rightValue);
						}

						// Check if rightValue is a direct boolean
						if (rightValue is bool rightBoolValue)
						{
							return leftBool == rightBoolValue;
						}
						// Check if rightValue is a JValue Boolean
						else if (rightValue is JValue rightBoolJValue && rightBoolJValue.Type == JTokenType.Boolean)
						{
							var rightBool = rightBoolJValue.Value<bool>();
							return leftBool == rightBool;
						}
						// Try to parse as boolean string
						else if (bool.TryParse(rightValue?.ToString(), out bool parsedBool))
						{
							return leftBool == parsedBool;
						}
					}
					return Equals(leftValue, rightValue);

				case BinaryOperator.NotEqual:
					// Handle string comparison for JValue objects
					if (leftValue is JValue leftJValueNE && leftJValueNE.Type == JTokenType.String)
					{
						string leftStr = leftJValueNE.Value<string>();
						string rightStr = rightValue is JValue rightJValueNE && rightJValueNE.Type == JTokenType.String
							? rightJValueNE.Value<string>()
							: rightValue as string;

						if (rightStr != null)
						{
							if (_logger != null)
							{
								_logger.LogDebug("JValue string not-equals comparison: '{left}' != '{right}'", leftStr, rightStr);
							}
							return !string.Equals(leftStr, rightStr, StringComparison.Ordinal);
						}
					}

					return !Equals(leftValue, rightValue);

				case BinaryOperator.GreaterThan:
					// Handle numeric comparisons for JValue objects
					if ((leftValue is JValue leftNumJValueGT &&
						(leftNumJValueGT.Type == JTokenType.Integer || leftNumJValueGT.Type == JTokenType.Float)) ||
						IsNumeric(leftValue))
					{
						// Extract the numeric value from JValue if needed
						double leftNum;
						if (leftValue is JValue leftJNum)
						{
							leftNum = leftJNum.Value<double>();
						}
						else
						{
							leftNum = Convert.ToDouble(leftValue);
						}

						// Extract the numeric value from right side
						double rightNum;
						if (rightValue is JValue rightJNum &&
							(rightJNum.Type == JTokenType.Integer || rightJNum.Type == JTokenType.Float))
						{
							rightNum = rightJNum.Value<double>();
						}
						else if (IsNumeric(rightValue))
						{
							rightNum = Convert.ToDouble(rightValue);
						}
						else if (double.TryParse(rightValue?.ToString(), out double parsedNum))
						{
							rightNum = parsedNum;
						}
						else
						{
							// If right value is not numeric, we can't compare
							return false;
						}

						if (_logger != null)
						{
							_logger.LogDebug("Numeric GT comparison: {left} > {right}", leftNum, rightNum);
						}

						return leftNum > rightNum;
					}
					return CompareValues(leftValue, rightValue) > 0;

				case BinaryOperator.LessThan:
					// Handle numeric comparisons for JValue objects
					if ((leftValue is JValue leftNumJValueLT &&
						(leftNumJValueLT.Type == JTokenType.Integer || leftNumJValueLT.Type == JTokenType.Float)) ||
						IsNumeric(leftValue))
					{
						// Extract the numeric value from JValue if needed
						double leftNum;
						if (leftValue is JValue leftJNum)
						{
							leftNum = leftJNum.Value<double>();
						}
						else
						{
							leftNum = Convert.ToDouble(leftValue);
						}

						// Extract the numeric value from right side
						double rightNum;
						if (rightValue is JValue rightJNum &&
							(rightJNum.Type == JTokenType.Integer || rightJNum.Type == JTokenType.Float))
						{
							rightNum = rightJNum.Value<double>();
						}
						else if (IsNumeric(rightValue))
						{
							rightNum = Convert.ToDouble(rightValue);
						}
						else if (double.TryParse(rightValue?.ToString(), out double parsedNum))
						{
							rightNum = parsedNum;
						}
						else
						{
							// If right value is not numeric, we can't compare
							return false;
						}

						if (_logger != null)
						{
							_logger.LogDebug("Numeric LT comparison: {left} < {right}", leftNum, rightNum);
						}

						return leftNum < rightNum;
					}
					return CompareValues(leftValue, rightValue) < 0;

				case BinaryOperator.GreaterThanOrEqual:
					// Handle numeric comparisons for JValue objects
					if ((leftValue is JValue leftNumJValueGTE &&
						(leftNumJValueGTE.Type == JTokenType.Integer || leftNumJValueGTE.Type == JTokenType.Float)) ||
						IsNumeric(leftValue))
					{
						// Extract the numeric value from JValue if needed
						double leftNum;
						if (leftValue is JValue leftJNum)
						{
							leftNum = leftJNum.Value<double>();
						}
						else
						{
							leftNum = Convert.ToDouble(leftValue);
						}

						// Extract the numeric value from right side
						double rightNum;
						if (rightValue is JValue rightJNum &&
							(rightJNum.Type == JTokenType.Integer || rightJNum.Type == JTokenType.Float))
						{
							rightNum = rightJNum.Value<double>();
						}
						else if (IsNumeric(rightValue))
						{
							rightNum = Convert.ToDouble(rightValue);
						}
						else if (double.TryParse(rightValue?.ToString(), out double parsedNum))
						{
							rightNum = parsedNum;
						}
						else
						{
							// If right value is not numeric, we can't compare
							return false;
						}

						if (_logger != null)
						{
							_logger.LogDebug("Numeric GTE comparison: {left} >= {right}", leftNum, rightNum);
						}

						return leftNum >= rightNum;
					}
					return CompareValues(leftValue, rightValue) >= 0;

				case BinaryOperator.LessThanOrEqual:
					// Handle numeric comparisons for JValue objects
					if ((leftValue is JValue leftNumJValueLTE &&
						(leftNumJValueLTE.Type == JTokenType.Integer || leftNumJValueLTE.Type == JTokenType.Float)) ||
						IsNumeric(leftValue))
					{
						// Extract the numeric value from JValue if needed
						double leftNum;
						if (leftValue is JValue leftJNum)
						{
							leftNum = leftJNum.Value<double>();
						}
						else
						{
							leftNum = Convert.ToDouble(leftValue);
						}

						// Extract the numeric value from right side
						double rightNum;
						if (rightValue is JValue rightJNum &&
							(rightJNum.Type == JTokenType.Integer || rightJNum.Type == JTokenType.Float))
						{
							rightNum = rightJNum.Value<double>();
						}
						else if (IsNumeric(rightValue))
						{
							rightNum = Convert.ToDouble(rightValue);
						}
						else if (double.TryParse(rightValue?.ToString(), out double parsedNum))
						{
							rightNum = parsedNum;
						}
						else
						{
							// If right value is not numeric, we can't compare
							return false;
						}

						if (_logger != null)
						{
							_logger.LogDebug("Numeric LTE comparison: {left} <= {right}", leftNum, rightNum);
						}

						return leftNum <= rightNum;
					}
					return CompareValues(leftValue, rightValue) <= 0;

				case BinaryOperator.And:
					return EvaluateExpression(item, binary.Left, parameters) && EvaluateExpression(item, binary.Right, parameters);

				case BinaryOperator.Or:
					return EvaluateExpression(item, binary.Left, parameters) || EvaluateExpression(item, binary.Right, parameters);

				case BinaryOperator.Between:
					if (binary.Left is PropertyExpression propExpression && binary.Right is BetweenExpression betweenExpr)
					{
						// Get the property value
						var propValue = GetPropertyValue(item, propExpression.PropertyPath);

						// Convert to JToken if needed
						JToken jPropValue = propValue as JToken ?? JToken.FromObject(propValue);

						// Get the lower and upper bounds
						var lowerBound = EvaluateValue(item, betweenExpr.LowerBound, parameters);
						var upperBound = EvaluateValue(item, betweenExpr.UpperBound, parameters);

						if (_logger != null)
						{
							_logger.LogDebug("Evaluating BETWEEN: {prop} BETWEEN {lower} AND {upper}",
								jPropValue, lowerBound, upperBound);
						}

						// Direct implementation of BETWEEN logic instead of using EvaluateCondition
						// This avoids potential issues with JArray serialization

						// Extract numeric values for comparison
						if (jPropValue.Type == JTokenType.Integer || jPropValue.Type == JTokenType.Float)
						{
							double propNum = jPropValue.Value<double>();
							double lowerNum = lowerBound is JToken jLower ? jLower.Value<double>() : Convert.ToDouble(lowerBound);
							double upperNum = upperBound is JToken jUpper ? jUpper.Value<double>() : Convert.ToDouble(upperBound);

							if (_logger != null)
							{
								_logger.LogDebug("Numeric BETWEEN comparison: {lower} <= {value} <= {upper}",
									lowerNum, propNum, upperNum);
							}

							return lowerNum <= propNum && propNum <= upperNum;
						}
						else if (jPropValue.Type == JTokenType.Date)
						{
							DateTime propDate = jPropValue.Value<DateTime>();
							DateTime lowerDate = lowerBound is JToken jLower ? jLower.Value<DateTime>() : Convert.ToDateTime(lowerBound);
							DateTime upperDate = upperBound is JToken jUpper ? jUpper.Value<DateTime>() : Convert.ToDateTime(upperBound);

							if (_logger != null)
							{
								_logger.LogDebug("DateTime BETWEEN comparison: {lower} <= {value} <= {upper}",
									lowerDate, propDate, upperDate);
							}

							return lowerDate <= propDate && propDate <= upperDate;
						}
						else if (jPropValue.Type == JTokenType.String)
						{
							string propStr = jPropValue.Value<string>();
							string lowerStr = lowerBound is JToken jLower ? jLower.Value<string>() : Convert.ToString(lowerBound);
							string upperStr = upperBound is JToken jUpper ? jUpper.Value<string>() : Convert.ToString(upperBound);

							if (_logger != null)
							{
								_logger.LogDebug("String BETWEEN comparison: {lower} <= {value} <= {upper}",
									lowerStr, propStr, upperStr);
							}

							return string.Compare(lowerStr, propStr, false) <= 0 &&
								   string.Compare(propStr, upperStr, false) <= 0;
						}
						else
						{
							// For other types, convert to string and compare
							string propStr = jPropValue.ToString();
							string lowerStr = lowerBound?.ToString() ?? string.Empty;
							string upperStr = upperBound?.ToString() ?? string.Empty;

							if (_logger != null)
							{
								_logger.LogDebug("String fallback BETWEEN comparison: {lower} <= {value} <= {upper}",
									lowerStr, propStr, upperStr);
							}

							return string.Compare(lowerStr, propStr, false) <= 0 &&
								   string.Compare(propStr, upperStr, false) <= 0;
						}
					}
					return false;

				default:
					throw new NotImplementedException($"Operator {binary.Operator} not implemented");
			}
		}

		if (expression is PropertyExpression prop)
		{
			// For boolean property expressions, simply check if the property exists and is true
			var value = GetPropertyValue(item, prop.PropertyPath);
			if (value is JValue jValue)
			{
				value = jValue.Value;
				if (_logger != null)
				{
					_logger.LogDebug("Converted JValue to underlying value: {value} (Type: {type})",
						value?.ToString() ?? "null", value?.GetType().Name ?? "null");
				}
			}
			if (value is bool boolValue)
			{
				return boolValue;
			}

			// For non-boolean property expressions in a boolean context, check if not null
			return value != null;
		}

		if (expression is ParameterExpression param)
		{
			// Look up the parameter value in the parameters collection
			if (parameters != null)
			{
				var paramMatch = parameters.FirstOrDefault(p => p.Name == param.ParameterName);
				if (paramMatch != default)
				{
					if (_logger != null)
					{
						_logger.LogDebug("Found parameter value for {paramName}: {value}",
							param.ParameterName, paramMatch.Value?.ToString() ?? "null");
					}

					// Convert to boolean context if needed
					if (paramMatch.Value is bool boolValue)
					{
						return boolValue;
					}
					return paramMatch.Value != null;
				}
			}

			if (_logger != null)
			{
				_logger.LogWarning("Parameter not found in supplied parameters: {paramName}", param.ParameterName);
			}

			// Parameters in boolean context should evaluate to false when not found
			return false;
		}

		if (expression is FunctionCallExpression func)
		{
			return EvaluateFunction(item, func, parameters);
		}
		else if (expression is UnaryExpression unary)
		{
			if (_logger != null)
			{
				_logger.LogDebug("Evaluating unary expression: {op}", unary.Operator);
			}

			object operandValue = EvaluateExpression(item, unary.Operand, parameters);

			switch (unary.Operator)
			{
				case UnaryOperator.Not:
					// Only handle boolean values directly
					if (operandValue is bool boolVal)
					{
						bool result = !boolVal;
						if (_logger != null)
						{
							_logger.LogDebug("NOT operator on boolean value {value} returned {result}", boolVal, result);
							_logger.LogDebug("NOT operator: operand value {value} is of type {type}", operandValue, operandValue?.GetType().Name);
						}
						return result;
					}

					// If it's not a boolean, throw an exception for clarity
					throw new InvalidOperationException($"NOT operator can only be applied to boolean values, but got {operandValue?.GetType().Name ?? "null"}");

				default:
					throw new NotImplementedException($"Unary operator {unary.Operator} not implemented");
			}
		}

		throw new NotImplementedException($"Expression type {expression.GetType().Name} not implemented");
	}

	private object EvaluateValue(JObject item, Expression expression, IReadOnlyList<(string Name, object Value)> parameters = null)
	{
		if (_logger != null)
		{
			_logger.LogDebug("Evaluating expression of type: {type}", expression.GetType().Name);
		}

		if (expression is ConstantExpression constant)
		{
			if (_logger != null)
			{
				_logger.LogDebug("Constant value: '{value}' (Type: {type})", constant.Value?.ToString() ?? "null", constant.Value?.GetType().Name ?? "null");
			}

			return constant.Value;
		}

		if (expression is PropertyExpression prop)
		{
			var propValue = GetPropertyValue(item, prop.PropertyPath);
			if (propValue is JValue jValue)
			{
				propValue = jValue.Value;
				if (_logger != null)
				{
					_logger.LogDebug("Converted JValue to underlying value: {value} (Type: {type})",
						propValue?.ToString() ?? "null", propValue?.GetType().Name ?? "null");
				}
			}
			if (_logger != null)
			{
				_logger.LogDebug("Property '{path}' value: '{value}' (Type: {type})",
					prop.PropertyPath, propValue?.ToString() ?? "null", propValue?.GetType().Name ?? "null");
			}

			return propValue;
		}

		if (expression is ParameterExpression param)
		{
			// Look up the parameter value in the parameters collection
			if (parameters != null)
			{
				var paramMatch = parameters.FirstOrDefault(p => p.Name == $"@{param.ParameterName}");
				if (paramMatch != default)
				{
					if (_logger != null)
					{
						_logger.LogDebug("Found parameter value for {paramName} in value context: {value}",
							param.ParameterName, paramMatch.Value?.ToString() ?? "null");
					}
					return paramMatch.Value;
				}
			}

			if (_logger != null)
			{
				_logger.LogWarning("Parameter not found in supplied parameters when evaluated as value: {paramName}", param.ParameterName);
			}

			// Parameters in value context should be treated as DBNull when not found
			// This ensures comparisons with them will fail predictably
			return DBNull.Value;
		}

		if (expression is FunctionCallExpression func)
		{
			if (_logger != null)
			{
				_logger.LogDebug("Evaluating function: {name}", func.Name);
			}

			return EvaluateFunction(item, func, parameters);
		}

		if (expression is BinaryExpression binary)
		{
			// For binary expressions in a value context, we evaluate them as boolean
			bool result = EvaluateExpression(item, binary, parameters);
			if (_logger != null)
			{
				_logger.LogDebug("Binary expression evaluated to: {result}", result);
			}
			return result;
		}

		if (expression is UnaryExpression unary)
		{
			// For unary expressions in a value context, we evaluate them as boolean
			bool result = EvaluateExpression(item, unary, parameters);
			if (_logger != null)
			{
				_logger.LogDebug("Unary expression evaluated to: {result}", result);
			}
			return result;
		}

		if (expression is BetweenExpression betweenExpression)
		{
			// For a BetweenExpression, return an array with the lower and upper bounds
			var lowerBound = EvaluateValue(item, betweenExpression.LowerBound, parameters);
			var upperBound = EvaluateValue(item, betweenExpression.UpperBound, parameters);

			if (_logger != null)
			{
				_logger.LogDebug("BetweenExpression bounds: [{lower}, {upper}]",
					lowerBound?.ToString() ?? "null",
					upperBound?.ToString() ?? "null");
			}

			return new JArray(lowerBound, upperBound);
		}

		throw new NotImplementedException($"Value expression type {expression.GetType().Name} not implemented");
	}

	private bool EvaluateFunction(JObject item, FunctionCallExpression function, IReadOnlyList<(string Name, object Value)> parameters = null)
	{
		if (_logger != null)
		{
			_logger.LogDebug("Evaluating function: {name} with {count} arguments",
				function.Name, function.Arguments.Count);
		}

		switch (function.Name.ToUpperInvariant())
		{
			case "CONTAINS":
				if (function.Arguments.Count < 2 || function.Arguments.Count > 3)
				{
					throw new ArgumentException("CONTAINS function requires 2 or 3 arguments");
				}

				var containsPropertyValue = EvaluateValue(item, function.Arguments[0], parameters)?.ToString();
				var containsSearchValue = EvaluateValue(item, function.Arguments[1], parameters)?.ToString();

				// Third argument is an optional boolean for case insensitivity
				// When set to true, CONTAINS performs a case-insensitive search
				// When unspecified, this value defaults to false (case-sensitive)
				var ignoreCase = false;
				if (function.Arguments.Count == 3)
				{
					var caseInsensitiveArg = EvaluateValue(item, function.Arguments[2], parameters);
					if (caseInsensitiveArg != null && bool.TryParse(caseInsensitiveArg.ToString(), out bool ignoreResult))
					{
						ignoreCase = ignoreResult;
					}
				}

				if (containsPropertyValue == null || containsSearchValue == null)
				{
					return false;
				}

				return ignoreCase
					? containsPropertyValue.IndexOf(containsSearchValue, StringComparison.OrdinalIgnoreCase) >= 0
					: containsPropertyValue.Contains(containsSearchValue);

			case "STARTSWITH":
				if (function.Arguments.Count != 2)
				{
					throw new ArgumentException("STARTSWITH function requires exactly 2 arguments");
				}

				var startsWithPropertyValue = EvaluateValue(item, function.Arguments[0], parameters)?.ToString();
				var startsWithSearchValue = EvaluateValue(item, function.Arguments[1], parameters)?.ToString();

				if (startsWithPropertyValue == null || startsWithSearchValue == null)
				{
					return false;
				}

				return startsWithPropertyValue.StartsWith(startsWithSearchValue);

			case "ARRAY_CONTAINS":
				if (function.Arguments.Count != 2)
				{
					throw new ArgumentException("ARRAY_CONTAINS function requires exactly 2 arguments");
				}

				var arrayValue = EvaluateValue(item, function.Arguments[0], parameters);
				var searchValue = EvaluateValue(item, function.Arguments[1], parameters);

				if (arrayValue == null || searchValue == null)
				{
					return false;
				}

				if (_logger != null)
				{
					_logger.LogDebug("ARRAY_CONTAINS: Checking if array {array} contains value {value}",
						arrayValue, searchValue);
				}

				// Handle JArray type
				if (arrayValue is JArray jArray)
				{
					// Convert search value to string for comparison if it's not null
					var searchValueString = searchValue?.ToString();

					foreach (var element in jArray)
					{
						if (element.ToString().Equals(searchValueString, StringComparison.OrdinalIgnoreCase))
						{
							if (_logger != null)
							{
								_logger.LogDebug("ARRAY_CONTAINS: Found match for {value} in array", searchValue);
							}
							return true;
						}
					}

					if (_logger != null)
					{
						_logger.LogDebug("ARRAY_CONTAINS: No match found for {value} in array", searchValue);
					}
					return false;
				}

				// Handle regular array types
				if (arrayValue is Array array)
				{
					foreach (var element in array)
					{
						if (element.Equals(searchValue))
						{
							if (_logger != null)
							{
								_logger.LogDebug("ARRAY_CONTAINS: Found match for {value} in array", searchValue);
							}
							return true;
						}
					}

					if (_logger != null)
					{
						_logger.LogDebug("ARRAY_CONTAINS: No match found for {value} in array", searchValue);
					}
					return false;
				}

				// If it's neither a JArray nor a regular array, return false
				if (_logger != null)
				{
					_logger.LogDebug("ARRAY_CONTAINS: Value is not an array: {value} (Type: {type})",
						arrayValue, arrayValue.GetType().Name);
				}
				return false;

			case "IS_NULL":
				if (function.Arguments.Count != 1)
				{
					throw new ArgumentException("IS_NULL function requires exactly 1 argument");
				}

				if (function.Arguments[0] is PropertyExpression propExpr)
				{
					var token = GetPropertyByPath(item, propExpr.PropertyPath);
					bool isNull = token == null || token.Type == JTokenType.Null;

					if (_logger != null)
					{
						_logger.LogDebug("IS_NULL: Property '{path}' is {result}", propExpr.PropertyPath, isNull ? "null" : "not null");
					}

					return isNull;
				}

				var argValue = EvaluateValue(item, function.Arguments[0], parameters);
				bool argIsNull = argValue == null ||
								 argValue == DBNull.Value ||
								 (argValue is JValue jv && jv.Type == JTokenType.Null);

				if (_logger != null)
				{
					_logger.LogDebug("IS_NULL: Value is {result}", argIsNull ? "null" : "not null");
				}

				return argIsNull;

			case "IS_DEFINED":
				if (function.Arguments.Count != 1)
				{
					throw new ArgumentException("IS_DEFINED function requires exactly 1 argument");
				}

				if (function.Arguments[0] is PropertyExpression isDefinedPropExpr)
				{
					var token = GetPropertyByPath(item, isDefinedPropExpr.PropertyPath);
					bool isDefined = token != null;

					if (_logger != null)
					{
						_logger.LogDebug("IS_DEFINED: Property '{path}' is {result}",
							isDefinedPropExpr.PropertyPath, isDefined ? "defined" : "not defined");
					}

					return isDefined;
				}

				var isDefinedArgValue = EvaluateValue(item, function.Arguments[0], parameters);
				bool isArgDefined = isDefinedArgValue != null && isDefinedArgValue != DBNull.Value;

				if (_logger != null)
				{
					_logger.LogDebug("IS_DEFINED: Value is {result}", isArgDefined ? "defined" : "not defined");
				}

				return isArgDefined;

			default:
				throw new NotImplementedException($"Function {function.Name} is not implemented");
		}
	}
}
