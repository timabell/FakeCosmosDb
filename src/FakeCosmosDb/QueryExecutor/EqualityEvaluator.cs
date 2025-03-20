using System;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace TimAbell.FakeCosmosDb.QueryExecutor;

/// <summary>
/// Evaluates equality between two values
/// </summary>
class EqualityEvaluator : IBinaryOperatorEvaluator
{
	private readonly ILogger _logger;

	public EqualityEvaluator(ILogger logger)
	{
		_logger = logger;
	}

	public override bool Evaluate(object left, object right)
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
