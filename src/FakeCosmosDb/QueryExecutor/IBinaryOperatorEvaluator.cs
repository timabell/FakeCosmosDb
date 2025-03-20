using System;
using Newtonsoft.Json.Linq;

namespace TimAbell.FakeCosmosDb.QueryExecutor;

/// <summary>
/// Interface for the binary operator evaluation strategy pattern
/// </summary>
abstract class IBinaryOperatorEvaluator
{
	/// <summary>
	/// Evaluates the binary operator with the given operands
	/// </summary>
	/// <param name="left">Left operand</param>
	/// <param name="right">Right operand</param>
	/// <returns>Result of the binary operation</returns>
	public abstract bool Evaluate(object left, object right);

	/// <summary>
	/// Extracts a numeric value from an object, handling different numeric representations.
	/// </summary>
	/// <param name="value">The value to extract from</param>
	/// <returns>The numeric value as a double, or null if not numeric</returns>
	protected static double? ExtractNumericValue(object value)
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

		if (Helpers.IsNumeric(value))
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

}
