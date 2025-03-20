using Microsoft.Extensions.Logging;

namespace TimAbell.FakeCosmosDb.QueryExecutor;

/// <summary>
/// Evaluates inequality between two values
/// </summary>
class InequalityEvaluator : IBinaryOperatorEvaluator
{
	private readonly EqualityEvaluator _equalityEvaluator;

	public InequalityEvaluator(ILogger logger)
	{
		_equalityEvaluator = new EqualityEvaluator(logger);
	}

	public override bool Evaluate(object left, object right)
	{
		return !_equalityEvaluator.Evaluate(left, right);
	}
}
