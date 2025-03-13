// Executes queries on in-memory data
public class CosmosDbQueryExecutor
{
    public static IEnumerable<JObject> Execute(ParsedQuery query, Dictionary<string, JObject> store)
    {
        var filtered = store.Values.AsQueryable();

        if (query.WhereClause != null)
        {
            filtered = filtered.Where(e => ApplyWhere(e, query.WhereClause));
        }

        if (query.OrderBy != null)
        {
            filtered = filtered.OrderBy(e => e[query.OrderBy]);
        }

        return filtered.ToList();
    }

    private static bool ApplyWhere(JObject entity, string condition)
    {
        var parts = condition.Split('=');
        var field = parts[0].Trim();
        var value = parts[1].Trim().Trim('\'');

        return entity[field]?.ToString().Equals(value, StringComparison.OrdinalIgnoreCase) ?? false;
    }
}
