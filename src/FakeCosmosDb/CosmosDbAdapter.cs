// Adapter for real CosmosDB

using System.Threading;
using System.Threading.Tasks;
using Azure.Identity;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;

namespace TimAbell.FakeCosmosDb;

/// <summary>
/// Optional. Provides concrete implementation of <see cref="ICosmosDb"/> that connects to real CosmosClient.
/// You can either use this if you want to depend on ICosmosDb to add some separation,
/// or just depend on directly on the real CosmosClient and FakeCosmosDb will be a drop-in replacement.
/// </summary>
public class CosmosDbAdapter : ICosmosDb
{
	private readonly CosmosClient _cosmosClient;

	public CosmosDbAdapter(CosmosClient cosmosClient)
	{
		_cosmosClient = cosmosClient;
	}
	public CosmosDbAdapter(string connectionString, ILogger logger = null, string databaseId = "TestDb")
		: this(connectionString, null, logger, databaseId)
	{
	}

	public CosmosDbAdapter(string connectionString, CosmosClientOptions clientOptions = null, ILogger logger = null, string databaseId = "TestDb")
	{
		logger?.LogInformation("Initializing CosmosClient with connection to {databaseId}", databaseId);
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
		return _cosmosClient.CreateDatabaseStreamAsync(databaseProperties, throughput, requestOptions, cancellationToken);
	}

	public FeedIterator<T> GetDatabaseQueryIterator<T>(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		return _cosmosClient.GetDatabaseQueryIterator<T>(queryDefinition, continuationToken, requestOptions);
	}

	public FeedIterator<T> GetDatabaseQueryIterator<T>(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		return _cosmosClient.GetDatabaseQueryIterator<T>(queryText, continuationToken, requestOptions);
	}

	public FeedIterator GetDatabaseQueryStreamIterator(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		return _cosmosClient.GetDatabaseQueryStreamIterator(queryDefinition, continuationToken, requestOptions);
	}

	public Task<AccountProperties> ReadAccountAsync()
	{
		return _cosmosClient.ReadAccountAsync();
	}

	public FeedIterator GetDatabaseQueryStreamIterator(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		return _cosmosClient.GetDatabaseQueryStreamIterator(queryText, continuationToken, requestOptions);
	}

	public Task<DatabaseResponse> CreateDatabaseIfNotExistsAsync(string id, ThroughputProperties throughputProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		return _cosmosClient.CreateDatabaseIfNotExistsAsync(id, throughputProperties, requestOptions, cancellationToken);
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
		return _cosmosClient.CreateDatabaseAsync(id, throughputProperties, requestOptions, cancellationToken);
	}

	public Task<DatabaseResponse> CreateDatabaseAsync(string databaseName, int? throughput = null, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		return _cosmosClient.CreateDatabaseAsync(databaseName, throughput, requestOptions, cancellationToken);
	}
}
