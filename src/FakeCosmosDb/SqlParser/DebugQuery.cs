namespace TimAbell.FakeCosmosDb.SqlParser;

public class DebugQuery
{
	/// <summary>
	/// Dumps the current state of the parser for diagnostic purposes.
	/// </summary>
	public static string DumpDebugInfo(string query)
	{
		var sb = new System.Text.StringBuilder();
		sb.AppendLine($"Original query: {query}");

		var parsedQuery = CosmosDbSqlGrammar.ParseQuery(query);
		sb.AppendLine($"AST: {parsedQuery}");
		return sb.ToString();
	}
}
