using Newtonsoft.Json.Linq;

namespace TimAbell.FakeCosmosDb.SqlParser;

/// <summary>
/// Represents a condition in a WHERE clause.
/// </summary>
public class WhereCondition
{
	/// <summary>
	/// The property path to test.
	/// </summary>
	public string PropertyPath { get; set; }
	
	/// <summary>
	/// The operator to apply.
	/// </summary>
	public ComparisonOperator Operator { get; set; }
	
	/// <summary>
	/// The value to compare with.
	/// </summary>
	public JToken Value { get; set; }
	
	/// <summary>
	/// The parameter name to compare with.
	/// </summary>
	public string ParameterName { get; set; }
	
	/// <summary>
	/// For CONTAINS function, indicates whether the search should be case-insensitive.
	/// When true, the search is case-insensitive. When false or null, the search is case-sensitive.
	/// </summary>
	public bool? IgnoreCase { get; set; }
}
