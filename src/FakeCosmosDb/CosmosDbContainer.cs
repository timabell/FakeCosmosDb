using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using TimAbell.FakeCosmosDb.Parsing;

namespace TimAbell.FakeCosmosDb;

public class CosmosDbContainer
{
	private readonly List<JObject> _store = new List<JObject>();
	private readonly ICosmosDbIndexManager _indexManager = new CosmosDbIndexManager();
	private readonly ICosmosDbPaginationManager _paginationManager = new CosmosDbPaginationManager();
	private readonly CosmosDbSqlQueryParser _queryParser = new CosmosDbSqlQueryParser();
	private readonly CosmosDbQueryExecutor _queryExecutor;

	// Add a property to access the store
	public List<JObject> Documents => _store;

	public CosmosDbContainer(ILogger logger = null)
	{
		_queryExecutor = new CosmosDbQueryExecutor(logger);
	}

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
		var results = _queryExecutor.Execute(parsedQuery, _store);
		return Task.FromResult(results);
	}

	public Task<(IEnumerable<JObject> Results, string ContinuationToken)> QueryWithPaginationAsync(string sql, int maxItemCount, string continuationToken = null)
	{
		var parsedQuery = _queryParser.Parse(sql);
		var results = _queryExecutor.Execute(parsedQuery, _store);
		var (pagedResults, nextToken) = _paginationManager.GetPage(results, maxItemCount, continuationToken);
		return Task.FromResult((pagedResults, nextToken));
	}
}
