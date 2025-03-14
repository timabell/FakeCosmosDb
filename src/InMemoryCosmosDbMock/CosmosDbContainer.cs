// Provides an in-memory mock of a CosmosDB container

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace TimAbell.MockableCosmos;

public class CosmosDbContainer
{
    private readonly List<JObject> _store = new List<JObject>();
    private readonly ICosmosDbIndexManager _indexManager = new CosmosDbIndexManager();
    private readonly ICosmosDbPaginationManager _paginationManager = new CosmosDbPaginationManager();
    private readonly ICosmosDbQueryParser _queryParser = new CosmosDbQueryParser();

    public Task AddAsync(object entity)
    {
        var json = JObject.FromObject(entity);
        var id = json["id"]?.ToString() ?? throw new InvalidOperationException("Entity must have an 'id' property.");
        _store.Add(json);
        _indexManager.Index(json);
        return Task.CompletedTask;
    }

    public Task<IEnumerable<JObject>> QueryAsync(string sql)
    {
        var parsedQuery = _queryParser.Parse(sql);
        var results = CosmosDbQueryExecutor.Execute(parsedQuery, _store);
        return Task.FromResult(results);
    }

    public Task<(IEnumerable<JObject> Results, string ContinuationToken)> QueryWithPaginationAsync(string sql, int maxItemCount, string continuationToken = null)
    {
        var parsedQuery = _queryParser.Parse(sql);
        var results = CosmosDbQueryExecutor.Execute(parsedQuery, _store);
        var (pagedResults, nextToken) = _paginationManager.GetPage(results, maxItemCount, continuationToken);
        return Task.FromResult((pagedResults, nextToken));
    }
}