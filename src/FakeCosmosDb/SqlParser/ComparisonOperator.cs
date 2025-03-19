using System;

namespace TimAbell.FakeCosmosDb.SqlParser
{
	public enum ComparisonOperator
	{
		Equals,
		NotEquals,
		GreaterThan,
		GreaterThanOrEqual,
		LessThan,
		LessThanOrEqual,
		StringContains,
		StringStartsWith,
		IsDefined,
		ArrayContains
	}

	public static class ComparisonOperatorExtensions
	{
		public static ComparisonOperator ParseOperator(string operatorText)
		{
			if (string.IsNullOrWhiteSpace(operatorText))
			{
				throw new ArgumentException("Operator cannot be null or empty", nameof(operatorText));
			}

			switch (operatorText.ToUpperInvariant())
			{
				case "=":
					return ComparisonOperator.Equals;
				case "!=":
					return ComparisonOperator.NotEquals;
				case ">":
					return ComparisonOperator.GreaterThan;
				case ">=":
					return ComparisonOperator.GreaterThanOrEqual;
				case "<":
					return ComparisonOperator.LessThan;
				case "<=":
					return ComparisonOperator.LessThanOrEqual;
				case "CONTAINS":
					return ComparisonOperator.StringContains;
				case "STARTSWITH":
					return ComparisonOperator.StringStartsWith;
				default:
					throw new NotSupportedException($"Operator '{operatorText}' is not supported");
			}
		}

		public static string ToSqlString(this ComparisonOperator op)
		{
			switch (op)
			{
				case ComparisonOperator.Equals:
					return "=";
				case ComparisonOperator.NotEquals:
					return "!=";
				case ComparisonOperator.GreaterThan:
					return ">";
				case ComparisonOperator.GreaterThanOrEqual:
					return ">=";
				case ComparisonOperator.LessThan:
					return "<";
				case ComparisonOperator.LessThanOrEqual:
					return "<=";
				case ComparisonOperator.StringContains:
					return "CONTAINS";
				case ComparisonOperator.StringStartsWith:
					return "STARTSWITH";
				default:
					throw new NotSupportedException($"Unknown operator: {op}");
			}
		}
	}
}
