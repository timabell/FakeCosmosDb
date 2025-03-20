using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
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
	// Dictionary to store databases, keyed by database name
	private readonly Dictionary<string, FakeDatabase> _databases = new();

	private readonly ILogger _logger;

	public FakeCosmosDb()
	{
	}

	public FakeCosmosDb(ILogger logger)
	{
		_logger = logger;
	}

	// Helper method to get or create a database
	private FakeDatabase GetOrCreateDatabase(string databaseName)
	{
		if (_databases.TryGetValue(databaseName, out var existingDatabase))
		{
			return existingDatabase;
		}

		var newDatabase = new FakeDatabase(databaseName, _logger);
		_databases[databaseName] = newDatabase;
		return newDatabase;
	}

	public override Task<ResponseMessage> CreateDatabaseStreamAsync(DatabaseProperties databaseProperties, int? throughput = null, RequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override FeedIterator<T> GetDatabaseQueryIterator<T>(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		throw new NotImplementedException();
	}

	public override FeedIterator<T> GetDatabaseQueryIterator<T>(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		throw new NotImplementedException();
	}

	public override FeedIterator GetDatabaseQueryStreamIterator(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		throw new NotImplementedException();
	}

	public override Task<AccountProperties> ReadAccountAsync()
	{
		throw new NotImplementedException();
	}

	public override FeedIterator GetDatabaseQueryStreamIterator(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		throw new NotImplementedException();
	}

	public override Task<DatabaseResponse> CreateDatabaseIfNotExistsAsync(string id, ThroughputProperties throughputProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		var database = GetOrCreateDatabase(id);
		return Task.FromResult<DatabaseResponse>(new FakeDatabaseResponse(database));
	}

	public override Container GetContainer(string databaseName, string containerId)
	{
		// Get or create the database
		var database = GetOrCreateDatabase(databaseName);

		// Get or create the container within that database
		return database.GetOrCreateContainer(containerId);
	}

	public override Task<DatabaseResponse> CreateDatabaseIfNotExistsAsync(string databaseName, int? throughput = null, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		var database = GetOrCreateDatabase(databaseName);
		return Task.FromResult<DatabaseResponse>(new FakeDatabaseResponse(database));
	}

	public override Database GetDatabase(string databaseName)
	{
		return GetOrCreateDatabase(databaseName);
	}

	public override Task<DatabaseResponse> CreateDatabaseAsync(string id, ThroughputProperties throughputProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<DatabaseResponse> CreateDatabaseAsync(string databaseName, int? throughput = null, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		var database = GetOrCreateDatabase(databaseName);
		return Task.FromResult<DatabaseResponse>(new FakeDatabaseResponse(database));
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
	public FakeDatabaseResponse(FakeDatabase database)
	{
		Database = database;
	}

	public override Database Database { get; }
}
