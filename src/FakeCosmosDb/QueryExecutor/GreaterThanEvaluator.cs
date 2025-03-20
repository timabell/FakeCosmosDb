using System;
using Microsoft.Extensions.Logging;

namespace TimAbell.FakeCosmosDb.QueryExecutor;

/// <summary>
/// Evaluates greater than comparison between two values
/// </summary>
class GreaterThanEvaluator : IBinaryOperatorEvaluator
{
	private readonly ILogger _logger;
	private readonly Func<object, object, int> _comparer;

	public GreaterThanEvaluator(ILogger logger, Func<object, object, int> comparer)
	{
		_logger = logger;
		_comparer = comparer;
	}

	public override bool Evaluate(object left, object right)
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
