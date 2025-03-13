// Query parsing logic
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

public class CosmosDbQueryParser
{
    public static ParsedQuery Parse(string sql)
    {
        var selectClause = ExtractSelectClause(sql);
        var whereClause = ExtractWhereClause(sql);
        var orderBy = ExtractOrderBy(sql);
        var limit = ExtractLimit(sql);

        return new ParsedQuery(selectClause, whereClause, orderBy, limit);
    }

    private static string ExtractSelectClause(string sql)
    {
        // Default to SELECT * if not specified
        if (!sql.ToUpperInvariant().Contains("SELECT"))
            return "*";

        var match = Regex.Match(sql, @"SELECT\s+(.*?)\s+FROM", RegexOptions.IgnoreCase);
        return match.Success ? match.Groups[1].Value.Trim() : "*";
    }

    private static string ExtractWhereClause(string sql)
    {
        if (!sql.ToUpperInvariant().Contains("WHERE"))
            return null;

        var fromWherePart = sql.Split(new[] { "WHERE" }, StringSplitOptions.None)[1];

        // Check if there's an ORDER BY clause after the WHERE
        if (fromWherePart.ToUpperInvariant().Contains("ORDER BY"))
            return fromWherePart.Split(new[] { "ORDER BY" }, StringSplitOptions.None)[0].Trim();

        // Check if there's a LIMIT clause after the WHERE
        if (fromWherePart.ToUpperInvariant().Contains("OFFSET") || fromWherePart.ToUpperInvariant().Contains("LIMIT"))
        {
            var limitIndex = fromWherePart.ToUpperInvariant().IndexOf("OFFSET");
            if (limitIndex < 0)
                limitIndex = fromWherePart.ToUpperInvariant().IndexOf("LIMIT");

            return fromWherePart.Substring(0, limitIndex).Trim();
        }

        return fromWherePart.Trim();
    }

    private static string ExtractOrderBy(string sql)
    {
        if (!sql.ToUpperInvariant().Contains("ORDER BY"))
            return null;

        var orderByPart = sql.Split(new[] { "ORDER BY" }, StringSplitOptions.None)[1];

        // Check if there's a LIMIT or OFFSET clause after the ORDER BY
        if (orderByPart.ToUpperInvariant().Contains("OFFSET") || orderByPart.ToUpperInvariant().Contains("LIMIT"))
        {
            var limitIndex = orderByPart.ToUpperInvariant().IndexOf("OFFSET");
            if (limitIndex < 0)
                limitIndex = orderByPart.ToUpperInvariant().IndexOf("LIMIT");

            return orderByPart.Substring(0, limitIndex).Trim();
        }

        return orderByPart.Trim();
    }

    private static int? ExtractLimit(string sql)
    {
        var limitMatch = Regex.Match(sql, @"LIMIT\s+(\d+)", RegexOptions.IgnoreCase);
        if (limitMatch.Success && int.TryParse(limitMatch.Groups[1].Value, out var limit))
            return limit;

        return null;
    }
}

public class ParsedQuery
{
    public string SelectClause { get; }
    public string WhereClause { get; }
    public string OrderBy { get; }
    public int? Limit { get; }

    public ParsedQuery(string selectClause, string whereClause, string orderBy, int? limit = null)
    {
        SelectClause = selectClause;
        WhereClause = whereClause;
        OrderBy = orderBy;
        Limit = limit;
    }

    public bool IsSelectAll => SelectClause == "*";

    public IEnumerable<string> GetSelectedProperties()
    {
        if (IsSelectAll)
            return null;

        return SelectClause.Split(',').Select(p => p.Trim());
    }
}
