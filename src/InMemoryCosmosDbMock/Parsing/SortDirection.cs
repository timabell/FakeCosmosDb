using System;

namespace TimAbell.MockableCosmos.Parsing
{
	public enum SortDirection
	{
		Ascending,
		Descending
	}

	public static class SortDirectionExtensions
	{
		public static SortDirection ParseDirection(string directionText)
		{
			if (string.IsNullOrWhiteSpace(directionText))
			{
				return SortDirection.Ascending; // Default to ascending
			}

			return directionText.Equals("DESC", StringComparison.OrdinalIgnoreCase)
				? SortDirection.Descending
				: SortDirection.Ascending;
		}

		public static string ToSqlString(this SortDirection direction)
		{
			return direction == SortDirection.Descending ? "DESC" : "ASC";
		}
	}
}
