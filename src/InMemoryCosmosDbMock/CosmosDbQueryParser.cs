// Query parsing logic
public class CosmosDbQueryParser
{
    public static ParsedQuery Parse(string sql)
    {
        var whereClause = ExtractWhereClause(sql);
        var orderBy = ExtractOrderBy(sql);
        return new ParsedQuery(whereClause, orderBy);
    }

    private static string ExtractWhereClause(string sql) =>
        sql.Contains("WHERE") ? sql.Split("WHERE")[1].Split("ORDER BY")[0].Trim() : null;

    private static string ExtractOrderBy(string sql) =>
        sql.Contains("ORDER BY") ? sql.Split("ORDER BY")[1].Trim() : null;
}
