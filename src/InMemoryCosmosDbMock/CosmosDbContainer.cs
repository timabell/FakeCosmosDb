// Container simulation
public class CosmosDbContainer
{
    private readonly Dictionary<string, JObject> _store = new();
    private readonly CosmosDbIndexManager _indexManager = new();

    public Task AddAsync(object entity)
    {
        var json = JObject.FromObject(entity);
        var id = json["id"]?.ToString() ?? throw new InvalidOperationException("Entity must have an 'id' property.");
        _store[id] = json;
        _indexManager.Index(json);
        return Task.CompletedTask;
    }

    public Task<IEnumerable<JObject>> QueryAsync(string sql)
    {
        var parsedQuery = CosmosDbQueryParser.Parse(sql);
        return Task.FromResult(CosmosDbQueryExecutor.Execute(parsedQuery, _store));
    }
}
