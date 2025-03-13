// Container simulation

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace TimAbell.MockableCosmos;

public class CosmosDbContainer
{
    private readonly Dictionary<string, JObject> _store = new();
    private readonly CosmosDbIndexManager _indexManager = new();
    private readonly CosmosDbPaginationManager _paginationManager = new();

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
        var results = CosmosDbQueryExecutor.Execute(parsedQuery, _store);
        return Task.FromResult(results);
    }

    public Task<(IEnumerable<JObject> Results, string ContinuationToken)> QueryWithPaginationAsync(string sql, int maxItemCount, string continuationToken = null)
    {
        var parsedQuery = CosmosDbQueryParser.Parse(sql);
        var results = CosmosDbQueryExecutor.Execute(parsedQuery, _store);
        var (pagedResults, nextToken) = _paginationManager.GetPage(results, maxItemCount, continuationToken);
        return Task.FromResult((pagedResults, nextToken));
    }
}