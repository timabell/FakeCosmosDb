using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using TimAbell.FakeCosmosDb.SqlParser;

namespace TimAbell.FakeCosmosDb.QueryExecutor;

/// <summary>
/// Evaluates logical AND between two expressions
/// </summary>
class LogicalAndEvaluator : IBinaryOperatorEvaluator
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

	public override bool Evaluate(object left, object right)
	{
		// This is not used since we need to short-circuit evaluation
		return _expressionEvaluator(_item, _left, _parameters) && _expressionEvaluator(_item, _right, _parameters);
	}
}
