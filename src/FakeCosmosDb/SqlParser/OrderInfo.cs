namespace TimAbell.FakeCosmosDb.SqlParser;

/// <summary>
/// Represents an ORDER BY clause.
/// </summary>
public class OrderInfo
{
	/// <summary>
	/// The property path to order by.
	/// </summary>
	public string PropertyPath { get; set; }

	/// <summary>
	/// The direction to sort in (ASC or DESC).
	/// </summary>
	public SortDirection Direction { get; set; } = SortDirection.Ascending;
}
