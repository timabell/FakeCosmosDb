namespace TimAbell.FakeCosmosDb.QueryExecutor;

public class Helpers
{
	internal static bool IsNumeric(object value)
	{
		return value is sbyte || value is byte || value is short || value is ushort ||
			   value is int || value is uint || value is long || value is ulong ||
			   value is float || value is double || value is decimal;
	}
}
