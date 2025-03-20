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

	public IEnumerable<JObject> Execute(List<JObject> store, IReadOnlyList<(string Name, object Value)> parameters, CosmosDbSqlQuery queryAst)
	{
		if (_logger != null)
		{
			_logger.LogDebug("Executing query on {count} documents", store.Count);
			_logger.LogDebug("Query details - AST: {ast}", queryAst != null ? "Present" : "null");
		}

		var filtered = store.AsQueryable();

		// Apply WHERE if specified
		if (queryAst != null && queryAst.Where != null)
		{
			if (_logger != null)
			{
				_logger.LogDebug("Applying WHERE from AST");
			}

			filtered = filtered.Where(e => ApplyWhere(e, queryAst.Where.Condition, parameters));
		}
		else
		{
			if (_logger != null)
			{
				_logger.LogDebug("No WHERE conditions to apply");
			}
		}

		// Apply ORDER BY if specified
		if (queryAst != null && queryAst.OrderBy != null && queryAst.OrderBy.Items.Count > 0)
		{
			if (_logger != null)
			{
				_logger.LogDebug("Applying ORDER BY from AST");
			}

			foreach (var orderByItem in queryAst.OrderBy.Items)
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

		var results = filtered.ToList();
		if (_logger != null)
		{
			_logger.LogDebug("After filtering and ordering, got {count} results", results.Count);
		}

		// Apply TOP if specified
		if (queryAst?.Select?.Top != null)
		{
			if (_logger != null)
			{
				_logger.LogDebug("Applying TOP {top} from AST", queryAst.Select.Top.Value);
			}

			results = results.Take(queryAst.Select.Top.Value).ToList();
		}

		// Apply LIMIT if specified
		if (queryAst != null && queryAst.Limit != null)
		{
			if (_logger != null)
			{
				_logger.LogDebug("Applying LIMIT {limit} from AST", queryAst.Limit.Value);
			}

			results = results.Take(queryAst.Limit.Value).ToList();
		}

		if (_logger != null)
		{
			_logger.LogDebug("After limit, final results count: {count}", results.Count);
		}

		// Apply SELECT projection if not SELECT *
		if (queryAst != null && queryAst.Select != null && !queryAst.Select.IsSelectAll)
		{
			var properties = GetSelectedProperties(queryAst.Select);
			if (_logger != null)
			{
				_logger.LogDebug("Applying projection from AST for properties: {properties}", string.Join(", ", properties));
			}

			return ApplyProjection(results, properties);
		}

		return results;
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

	private static bool IsNumeric(object value)
	{
		return value is sbyte || value is byte || value is short || value is ushort ||
			   value is int || value is uint || value is long || value is ulong ||
			   value is float || value is double || value is decimal;
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

		if (_logger != null)
		{
			_logger.LogDebug("ApplyWhere result: {result} for item with id: {id}", boolResult, item["id"]);
		}

		return boolResult;
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

			var evaluator = CreateBinaryOperatorEvaluator(binary.Operator, item, binary.Left, binary.Right, parameters);
			return evaluator.Evaluate(leftValue, rightValue);
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

		if (expression is UnaryExpression unary)
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
					throw new NotSupportedException($"Unary operator {unary.Operator} not implemented");
			}
		}

		throw new NotSupportedException($"Expression type {expression.GetType().Name} not implemented");
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

		throw new NotSupportedException($"Value expression type {expression.GetType().Name} not implemented");
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
				throw new NotSupportedException($"Function {function.Name} is not implemented");
		}
	}

	/// <summary>
	/// Interface for the binary operator evaluation strategy pattern
	/// </summary>
	private interface IBinaryOperatorEvaluator
	{
		/// <summary>
		/// Evaluates the binary operator with the given operands
		/// </summary>
		/// <param name="left">Left operand</param>
		/// <param name="right">Right operand</param>
		/// <returns>Result of the binary operation</returns>
		bool Evaluate(object left, object right);
	}

	/// <summary>
	/// Evaluates equality between two values
	/// </summary>
	private class EqualityEvaluator : IBinaryOperatorEvaluator
	{
		private readonly ILogger _logger;

		public EqualityEvaluator(ILogger logger)
		{
			_logger = logger;
		}

		public bool Evaluate(object left, object right)
		{
			// Handle string comparison
			string leftStr = ExtractStringValue(left);
			string rightStr = ExtractStringValue(right);

			if (left is JValue leftJValue && leftJValue.Type == JTokenType.String &&
				(right is JValue rightJValue && rightJValue.Type == JTokenType.String || right is string))
			{
				if (_logger != null)
				{
					_logger.LogDebug("String equality comparison: '{left}' = '{right}'", leftStr, rightStr);
				}
				return string.Equals(leftStr, rightStr, StringComparison.Ordinal);
			}

			// Handle numeric comparisons
			double? leftNum = ExtractNumericValue(left);
			double? rightNum = ExtractNumericValue(right);

			if (leftNum.HasValue && rightNum.HasValue)
			{
				if (_logger != null)
				{
					_logger.LogDebug("Numeric equality comparison: {left} = {right}", leftNum.Value, rightNum.Value);
				}
				return Math.Abs(leftNum.Value - rightNum.Value) < 0.000001; // Use small epsilon for floating point comparison
			}

			// Handle boolean comparisons
			bool? leftBool = ExtractBooleanValue(left);
			bool? rightBool = ExtractBooleanValue(right);

			if (leftBool.HasValue && rightBool.HasValue)
			{
				if (_logger != null)
				{
					_logger.LogDebug("Boolean equality comparison: {left} = {right}", leftBool.Value, rightBool.Value);
				}
				return leftBool.Value == rightBool.Value;
			}

			// Default object equality
			return Equals(left, right);
		}
	}

	/// <summary>
	/// Evaluates inequality between two values
	/// </summary>
	private class InequalityEvaluator : IBinaryOperatorEvaluator
	{
		private readonly EqualityEvaluator _equalityEvaluator;

		public InequalityEvaluator(ILogger logger)
		{
			_equalityEvaluator = new EqualityEvaluator(logger);
		}

		public bool Evaluate(object left, object right)
		{
			return !_equalityEvaluator.Evaluate(left, right);
		}
	}

	/// <summary>
	/// Evaluates greater than comparison between two values
	/// </summary>
	private class GreaterThanEvaluator : IBinaryOperatorEvaluator
	{
		private readonly ILogger _logger;
		private readonly Func<object, object, int> _comparer;

		public GreaterThanEvaluator(ILogger logger, Func<object, object, int> comparer)
		{
			_logger = logger;
			_comparer = comparer;
		}

		public bool Evaluate(object left, object right)
		{
			double? leftNum = ExtractNumericValue(left);
			double? rightNum = ExtractNumericValue(right);

			if (leftNum.HasValue && rightNum.HasValue)
			{
				if (_logger != null)
				{
					_logger.LogDebug("Numeric GT comparison: {left} > {right}", leftNum.Value, rightNum.Value);
				}
				return leftNum.Value > rightNum.Value;
			}

			return _comparer(left, right) > 0;
		}
	}

	/// <summary>
	/// Evaluates less than comparison between two values
	/// </summary>
	private class LessThanEvaluator : IBinaryOperatorEvaluator
	{
		private readonly ILogger _logger;
		private readonly Func<object, object, int> _comparer;

		public LessThanEvaluator(ILogger logger, Func<object, object, int> comparer)
		{
			_logger = logger;
			_comparer = comparer;
		}

		public bool Evaluate(object left, object right)
		{
			double? leftNum = ExtractNumericValue(left);
			double? rightNum = ExtractNumericValue(right);

			if (leftNum.HasValue && rightNum.HasValue)
			{
				if (_logger != null)
				{
					_logger.LogDebug("Numeric LT comparison: {left} < {right}", leftNum.Value, rightNum.Value);
				}
				return leftNum.Value < rightNum.Value;
			}

			return _comparer(left, right) < 0;
		}
	}

	/// <summary>
	/// Evaluates greater than or equal comparison between two values
	/// </summary>
	private class GreaterThanOrEqualEvaluator : IBinaryOperatorEvaluator
	{
		private readonly ILogger _logger;
		private readonly Func<object, object, int> _comparer;

		public GreaterThanOrEqualEvaluator(ILogger logger, Func<object, object, int> comparer)
		{
			_logger = logger;
			_comparer = comparer;
		}

		public bool Evaluate(object left, object right)
		{
			double? leftNum = ExtractNumericValue(left);
			double? rightNum = ExtractNumericValue(right);

			if (leftNum.HasValue && rightNum.HasValue)
			{
				if (_logger != null)
				{
					_logger.LogDebug("Numeric GTE comparison: {left} >= {right}", leftNum.Value, rightNum.Value);
				}
				return leftNum.Value >= rightNum.Value;
			}

			return _comparer(left, right) >= 0;
		}
	}

	/// <summary>
	/// Evaluates less than or equal comparison between two values
	/// </summary>
	private class LessThanOrEqualEvaluator : IBinaryOperatorEvaluator
	{
		private readonly ILogger _logger;
		private readonly Func<object, object, int> _comparer;

		public LessThanOrEqualEvaluator(ILogger logger, Func<object, object, int> comparer)
		{
			_logger = logger;
			_comparer = comparer;
		}

		public bool Evaluate(object left, object right)
		{
			double? leftNum = ExtractNumericValue(left);
			double? rightNum = ExtractNumericValue(right);

			if (leftNum.HasValue && rightNum.HasValue)
			{
				if (_logger != null)
				{
					_logger.LogDebug("Numeric LTE comparison: {left} <= {right}", leftNum.Value, rightNum.Value);
				}
				return leftNum.Value <= rightNum.Value;
			}

			return _comparer(left, right) <= 0;
		}
	}

	/// <summary>
	/// Evaluates logical AND between two expressions
	/// </summary>
	private class LogicalAndEvaluator : IBinaryOperatorEvaluator
	{
		private readonly JObject _item;
		private readonly Expression _left;
		private readonly Expression _right;
		private readonly IReadOnlyList<(string Name, object Value)> _parameters;
		private readonly Func<JObject, Expression, IReadOnlyList<(string Name, object Value)>, bool> _expressionEvaluator;

		public LogicalAndEvaluator(
			JObject item,
			Expression left,
			Expression right,
			IReadOnlyList<(string Name, object Value)> parameters,
			Func<JObject, Expression, IReadOnlyList<(string Name, object Value)>, bool> expressionEvaluator)
		{
			_item = item;
			_left = left;
			_right = right;
			_parameters = parameters;
			_expressionEvaluator = expressionEvaluator;
		}

		public bool Evaluate(object left, object right)
		{
			// This is not used since we need to short-circuit evaluation
			return _expressionEvaluator(_item, _left, _parameters) && _expressionEvaluator(_item, _right, _parameters);
		}
	}

	/// <summary>
	/// Evaluates logical OR between two expressions
	/// </summary>
	private class LogicalOrEvaluator : IBinaryOperatorEvaluator
	{
		private readonly JObject _item;
		private readonly Expression _left;
		private readonly Expression _right;
		private readonly IReadOnlyList<(string Name, object Value)> _parameters;
		private readonly Func<JObject, Expression, IReadOnlyList<(string Name, object Value)>, bool> _expressionEvaluator;

		public LogicalOrEvaluator(
			JObject item,
			Expression left,
			Expression right,
			IReadOnlyList<(string Name, object Value)> parameters,
			Func<JObject, Expression, IReadOnlyList<(string Name, object Value)>, bool> expressionEvaluator)
		{
			_item = item;
			_left = left;
			_right = right;
			_parameters = parameters;
			_expressionEvaluator = expressionEvaluator;
		}

		public bool Evaluate(object left, object right)
		{
			// This is not used since we need to short-circuit evaluation
			return _expressionEvaluator(_item, _left, _parameters) || _expressionEvaluator(_item, _right, _parameters);
		}
	}

	/// <summary>
	/// Evaluates BETWEEN condition for a property value between two bounds
	/// </summary>
	private class BetweenEvaluator : IBinaryOperatorEvaluator
	{
		private readonly JObject _item;
		private readonly Expression _left;
		private readonly Expression _right;
		private readonly IReadOnlyList<(string Name, object Value)> _parameters;
		private readonly ILogger _logger;
		private readonly Func<JObject, string, object> _propertyGetter;
		private readonly Func<JObject, Expression, IReadOnlyList<(string Name, object Value)>, object> _valueEvaluator;

		public BetweenEvaluator(
			JObject item,
			Expression left,
			Expression right,
			IReadOnlyList<(string Name, object Value)> parameters,
			ILogger logger,
			Func<JObject, string, object> propertyGetter,
			Func<JObject, Expression, IReadOnlyList<(string Name, object Value)>, object> valueEvaluator)
		{
			_item = item;
			_left = left;
			_right = right;
			_parameters = parameters;
			_logger = logger;
			_propertyGetter = propertyGetter;
			_valueEvaluator = valueEvaluator;
		}

		public bool Evaluate(object left, object right)
		{
			// This method implementation doesn't use the parameters since we need to evaluate expressions differently
			if (_left is PropertyExpression propExpression && _right is BetweenExpression betweenExpr)
			{
				// Get the property value
				var propValue = _propertyGetter(_item, propExpression.PropertyPath);

				// Convert to JToken if needed
				JToken jPropValue = propValue as JToken ?? JToken.FromObject(propValue);

				// Get the lower and upper bounds
				var lowerBound = _valueEvaluator(_item, betweenExpr.LowerBound, _parameters);
				var upperBound = _valueEvaluator(_item, betweenExpr.UpperBound, _parameters);

				if (_logger != null)
				{
					_logger.LogDebug("Evaluating BETWEEN: {prop} BETWEEN {lower} AND {upper}",
						jPropValue, lowerBound, upperBound);
				}

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

				if (jPropValue.Type == JTokenType.Date)
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

				if (jPropValue.Type == JTokenType.String)
				{
					string propStr = jPropValue.Value<string>();
					string lowerStr = lowerBound is JToken jLower ? jLower.Value<string>() : Convert.ToString(lowerBound);
					string upperStr = upperBound is JToken jUpper ? jUpper.Value<string>() : Convert.ToString(upperBound);

					if (_logger != null)
					{
						_logger.LogDebug("String BETWEEN comparison: {lower} <= {value} <= {upper}",
							lowerStr, propStr, upperStr);
					}

					return string.Compare(lowerStr, propStr) <= 0 &&
							string.Compare(propStr, upperStr) <= 0;
				}
				else
				{
					// For other types, convert to string and compare
					string propStr = jPropValue.ToString();
					string lowerStr = lowerBound.ToString();
					string upperStr = upperBound.ToString();
					return string.Compare(lowerStr, propStr) <= 0 &&
							string.Compare(propStr, upperStr) <= 0;
				}
			}
			return false;
		}
	}

	/// <summary>
	/// Factory to create the appropriate binary operator evaluator based on the operator type
	/// </summary>
	private IBinaryOperatorEvaluator CreateBinaryOperatorEvaluator(
		BinaryOperator op,
		JObject item = null,
		Expression leftExpr = null,
		Expression rightExpr = null,
		IReadOnlyList<(string Name, object Value)> parameters = null)
	{
		switch (op)
		{
			case BinaryOperator.Equal:
				return new EqualityEvaluator(_logger);
			case BinaryOperator.NotEqual:
				return new InequalityEvaluator(_logger);
			case BinaryOperator.GreaterThan:
				return new GreaterThanEvaluator(_logger, CompareValues);
			case BinaryOperator.LessThan:
				return new LessThanEvaluator(_logger, CompareValues);
			case BinaryOperator.GreaterThanOrEqual:
				return new GreaterThanOrEqualEvaluator(_logger, CompareValues);
			case BinaryOperator.LessThanOrEqual:
				return new LessThanOrEqualEvaluator(_logger, CompareValues);
			case BinaryOperator.And:
				return new LogicalAndEvaluator(item, leftExpr, rightExpr, parameters, EvaluateExpression);
			case BinaryOperator.Or:
				return new LogicalOrEvaluator(item, leftExpr, rightExpr, parameters, EvaluateExpression);
			case BinaryOperator.Between:
				return new BetweenEvaluator(item, leftExpr, rightExpr, parameters, _logger, GetPropertyValue, EvaluateValue);
			default:
				throw new NotSupportedException($"Operator {op} not implemented");
		}
	}

	/// <summary>
	/// Extracts a typed value from an object, handling JValue conversions and type casting.
	/// </summary>
	/// <typeparam name="T">The expected return type</typeparam>
	/// <param name="value">The value to extract from</param>
	/// <returns>The extracted value as type T or default(T) if conversion fails</returns>
	private T ExtractValue<T>(object value)
	{
		if (value == null)
		{
			return default;
		}

		if (value is JValue jValue)
		{
			value = jValue.Value;
			if (_logger != null)
			{
				_logger.LogDebug("Extracted value from JValue: {value} (Type: {type})",
					value?.ToString() ?? "null", value?.GetType().Name ?? "null");
			}
		}

		if (value is T typedValue)
		{
			return typedValue;
		}

		try
		{
			return (T)Convert.ChangeType(value, typeof(T));
		}
		catch (Exception ex)
		{
			if (_logger != null)
			{
				_logger.LogDebug("Could not convert {value} to {type}: {error}",
					value, typeof(T).Name, ex.Message);
			}
			return default;
		}
	}

	/// <summary>
	/// Extracts a numeric value from an object, handling different numeric representations.
	/// </summary>
	/// <param name="value">The value to extract from</param>
	/// <returns>The numeric value as a double, or null if not numeric</returns>
	private static double? ExtractNumericValue(object value)
	{
		if (value == null)
		{
			return null;
		}

		if (value is JValue jValue)
		{
			if (jValue.Type == JTokenType.Integer || jValue.Type == JTokenType.Float)
			{
				return jValue.Value<double>();
			}
			value = jValue.Value;
		}

		if (IsNumeric(value))
		{
			return Convert.ToDouble(value);
		}

		double parsedNum;
		if (double.TryParse(value?.ToString(), out parsedNum))
		{
			return parsedNum;
		}

		return null;
	}

	/// <summary>
	/// Extracts a string value from an object, handling JValue string type.
	/// </summary>
	/// <param name="value">The value to extract from</param>
	/// <returns>The string value, or null if the input is null</returns>
	private static string ExtractStringValue(object value)
	{
		if (value == null)
		{
			return null;
		}

		if (value is JValue jValue && jValue.Type == JTokenType.String)
		{
			return jValue.Value<string>();
		}

		return value.ToString();
	}

	/// <summary>
	/// Extracts a boolean value from an object, handling various boolean representations.
	/// </summary>
	/// <param name="value">The value to extract from</param>
	/// <returns>The boolean value, or null if conversion fails</returns>
	private static bool? ExtractBooleanValue(object value)
	{
		if (value == null)
		{
			return null;
		}

		if (value is bool boolValue)
		{
			return boolValue;
		}

		if (value is JValue jValue && jValue.Type == JTokenType.Boolean)
		{
			return jValue.Value<bool>();
		}

		bool result;
		if (bool.TryParse(value.ToString(), out result))
		{
			return result;
		}

		return null;
	}
}
