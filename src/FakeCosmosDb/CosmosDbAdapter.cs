// Adapter for real CosmosDB

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Identity;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace TimAbell.FakeCosmosDb;

/// <summary>
/// Optional. Provides concrete implementation of <see cref="ICosmosDb"/> that connects to real CosmosClient.
/// You can either use this if you want to depend on ICosmosDb to add some separation,
/// or just depend on directly on the real CosmosClient and FakeCosmosDb will be a drop-in replacement.
/// </summary>
public class CosmosDbAdapter : ICosmosDb
{
	private readonly CosmosClient _cosmosClient;
	private readonly string _databaseId;
	private readonly ILogger _logger;

	public CosmosDbAdapter(string connectionString, ILogger logger = null, string databaseId = "TestDb")
		: this(connectionString, null, logger, databaseId)
	{
	}

	public CosmosDbAdapter(string connectionString, CosmosClientOptions clientOptions = null, ILogger logger = null, string databaseId = "TestDb")
	{
		_logger = logger;
		_databaseId = databaseId;

		_logger?.LogInformation("Initializing CosmosClient with connection to {databaseId}", databaseId);
		_cosmosClient = clientOptions != null
			? new CosmosClient(connectionString, clientOptions)
			: new CosmosClient(connectionString);
	}

	public CosmosDbAdapter(string cosmosOptionsAccountEndpoint, string cosmosOptionsAccountKey, CosmosClientOptions clientOptions)
	{
		_cosmosClient = new CosmosClient(cosmosOptionsAccountEndpoint, cosmosOptionsAccountKey, clientOptions);
	}

	public CosmosDbAdapter(string cosmosOptionsAccountEndpoint, DefaultAzureCredential cosmosOptionsAccountKey, CosmosClientOptions clientOptions)
	{
		_cosmosClient = new CosmosClient(cosmosOptionsAccountEndpoint, cosmosOptionsAccountKey, clientOptions);
	}

	public Task<ResponseMessage> CreateDatabaseStreamAsync(DatabaseProperties databaseProperties, int? throughput = null, RequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new System.NotImplementedException();
	}

	public FeedIterator<T> GetDatabaseQueryIterator<T>(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		throw new System.NotImplementedException();
	}

	public FeedIterator<T> GetDatabaseQueryIterator<T>(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		throw new System.NotImplementedException();
	}

	public FeedIterator GetDatabaseQueryStreamIterator(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		throw new System.NotImplementedException();
	}

	public Task<AccountProperties> ReadAccountAsync()
	{
		throw new System.NotImplementedException();
	}

	public FeedIterator GetDatabaseQueryStreamIterator(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		throw new System.NotImplementedException();
	}

	public Task<DatabaseResponse> CreateDatabaseIfNotExistsAsync(string id, ThroughputProperties throughputProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new System.NotImplementedException();
	}

	public Container GetContainer(string databaseName, string containerId)
	{
		return _cosmosClient.GetContainer(databaseName, containerId);
	}

	public async Task<DatabaseResponse> CreateDatabaseIfNotExistsAsync(string databaseName, int? throughput = null, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		return await _cosmosClient.CreateDatabaseIfNotExistsAsync(databaseName, throughput, requestOptions, cancellationToken);
	}

	public Database GetDatabase(string databaseName)
	{
		return _cosmosClient.GetDatabase(databaseName);
	}

	public Task<DatabaseResponse> CreateDatabaseAsync(string id, ThroughputProperties throughputProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new System.NotImplementedException();
	}

	public Task<DatabaseResponse> CreateDatabaseAsync(string databaseName, int? throughput = null, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		throw new System.NotImplementedException();
	}
}
