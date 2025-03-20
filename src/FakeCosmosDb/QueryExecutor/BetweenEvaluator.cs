using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using TimAbell.FakeCosmosDb.SqlParser;

namespace TimAbell.FakeCosmosDb.QueryExecutor;

/// <summary>
/// Evaluates BETWEEN condition for a property value between two bounds
/// </summary>
class BetweenEvaluator : IBinaryOperatorEvaluator
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

	public override bool Evaluate(object left, object right)
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
