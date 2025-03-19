using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using TimAbell.FakeCosmosDb.Implementation;
using TimAbell.FakeCosmosDb.SqlParser;

namespace TimAbell.FakeCosmosDb;

/// <summary>
/// Fake CosmosDb implementation that stores data in memory and allows round-trip of stored data.
/// Can be swapped in in two ways:
/// 1. Depend on the ICosmosDb interface and inject this class in place of the real CosmosDb, this is clearer but requires more changes to production code.
/// 2. Use the fact that this class inherits from the real CosmosClient class and use it as a drop-in replacement for the real CosmosClient.
/// It's a bit icky that we inherit from CosmosClient, but it allows us to be a drop-in replacement for existing production code that already depends on
/// CosmosClient without requiring more code changes. It would be nice if we could have added the missing interface to CosmosClient, but typically microsoft didn't
/// provide one and c# doesn't let you add interfaces to classes you don't own (unlike golang).
/// </summary>
public class FakeCosmosDb : CosmosClient, ICosmosDb
{
	private readonly Dictionary<string, InMemoryContainer> _containers = new();
	private readonly CosmosDbSqlQueryParser _queryParser;
	private readonly ILogger _logger;
	private readonly CosmosDbQueryExecutor _queryExecutor;

	public FakeCosmosDb()
	{
		_queryParser = new CosmosDbSqlQueryParser();
		_queryExecutor = new CosmosDbQueryExecutor();
	}

	public FakeCosmosDb(ILogger logger)
	{
		_logger = logger;
		_queryParser = new CosmosDbSqlQueryParser(_logger);
		_queryExecutor = new CosmosDbQueryExecutor(logger);
	}

	public Task AddContainerAsync(string containerName)
	{
		if (!_containers.ContainsKey(containerName))
			_containers[containerName] = new InMemoryContainer(_logger);
		return Task.CompletedTask;
	}

	public Task AddItemAsync(string containerName, object entity)
	{
		if (!_containers.ContainsKey(containerName))
			throw new InvalidOperationException($"Container '{containerName}' does not exist.");

		return _containers[containerName].AddAsync(entity);
	}

	public Task<IEnumerable<JObject>> QueryAsync(string containerName, string sql)
	{
		_logger?.LogDebug("Executing query '{sql}' on container '{containerName}'", sql, containerName);

		try
		{
			// Get the container
			if (!_containers.TryGetValue(containerName, out var container))
			{
				_logger?.LogWarning("Container '{containerName}' not found", containerName);
				throw new InvalidOperationException($"Container '{containerName}' not found");
			}

			// Parse the query
			_logger?.LogDebug("Parsing query");
			var parsedQuery = _queryParser.Parse(sql);
			_logger?.LogDebug("Query parsed successfully. WhereConditions: {count}",
				parsedQuery.WhereConditions != null ? parsedQuery.WhereConditions.Count.ToString() : "null");

			// Execute the query
			_logger?.LogDebug("Executing query against in-memory store");
			var results = _queryExecutor.Execute(parsedQuery, container.Documents);
			_logger?.LogDebug("Query execution complete. Results count: {count}", results.Count());

			return Task.FromResult<IEnumerable<JObject>>(results);
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error executing query: {message}", ex.Message);
			if (ex.InnerException != null)
			{
				_logger?.LogError(ex, "Inner exception: {message}", ex.InnerException.Message);
			}

			throw;
		}
	}

	public Task<(IEnumerable<JObject> Results, string ContinuationToken)> QueryWithPaginationAsync(string containerName, string sql, int maxItemCount, string continuationToken = null)
	{
		if (!_containers.ContainsKey(containerName))
			throw new InvalidOperationException($"Container '{containerName}' does not exist.");

		return _containers[containerName].QueryWithPaginationAsync(sql, maxItemCount, continuationToken);
	}

	public override Container GetContainer(string databaseName, string containerId)
	{
		return new FakeContainer(); // todo: connect to stored containers
	}

	public override Task<DatabaseResponse> CreateDatabaseIfNotExistsAsync(string databaseName, int? throughput = null, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		return Task.FromResult<DatabaseResponse>(new FakeDatabaseResponse(databaseName));
	}

	public override Database GetDatabase(string databaseName)
	{
		return new FakeDatabase(databaseName);
	}

	protected override void Dispose(bool disposing)
	{
		// No-op.
		// Don't call base.Dispose(disposing) to prevent NullReferenceException.
		// The base implementation tries to dispose resources we don't have.
	}
}

public class FakeDatabaseResponse : DatabaseResponse
{
	public FakeDatabaseResponse(string databaseName)
	{
		Database = new FakeDatabase(databaseName);
	}
	public override Database Database { get; }
}
