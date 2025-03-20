using System.Collections.Generic;

namespace TimAbell.FakeCosmosDb.SqlParser;

/// <summary>
/// Represents a parsed CosmosDB SQL query.
/// </summary>
public class ParsedQuery
{
	/// <summary>
	/// The parsed SQL query using the Sprache SQL parser.
	/// </summary>
	public CosmosDbSqlQuery SprachedSqlAst { get; set; }

	/// <summary>
	/// List of property paths to select from the results.
	/// </summary>
	public List<string> PropertyPaths { get; set; } = new();

	/// <summary>
	/// The name of the container or collection in the FROM clause.
	/// </summary>
	public string FromName { get; set; }

	/// <summary>
	/// The alias used for the FROM source, if any.
	/// </summary>
	public string FromAlias { get; set; }

	/// <summary>
	/// List of conditions in the WHERE clause.
	/// </summary>
	public List<WhereCondition> WhereConditions { get; set; } = new();

	/// <summary>
	/// The complete expression tree for the WHERE clause.
	/// This is used for complex expressions that can't be easily represented as simple conditions,
	/// such as those involving NOT operator or complex logical combinations.
	/// </summary>
	public Expression WhereExpression { get; set; }

	/// <summary>
	/// List of ORDER BY clauses.
	/// </summary>
	public List<OrderInfo> OrderBy { get; set; }

	/// <summary>
	/// LIMIT value, if any.
	/// </summary>
	public int Limit { get; set; }

	/// <summary>
	/// TOP value, if any.
	/// </summary>
	public int TopValue { get; set; }
}
