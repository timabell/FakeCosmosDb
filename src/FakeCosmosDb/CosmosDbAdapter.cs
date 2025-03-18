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

	public async Task AddContainerAsync(string containerName)
	{
		_logger?.LogInformation("Creating container {containerName} if it doesn't exist", containerName);
		await _cosmosClient.GetDatabase(_databaseId).CreateContainerIfNotExistsAsync(containerName, "/id");
	}

	public async Task AddItemAsync(string containerName, object entity)
	{
		_logger?.LogInformation("Adding item to container {containerName}", containerName);
		var container = _cosmosClient.GetContainer(_databaseId, containerName);
		await container.CreateItemAsync(entity);
	}

	public async Task<IEnumerable<JObject>> QueryAsync(string containerName, string sql)
	{
		_logger?.LogInformation("Executing query on container {containerName}: {sql}", containerName, sql);
		var container = _cosmosClient.GetContainer(_databaseId, containerName);
		var queryDefinition = new QueryDefinition(sql);
		var iterator = container.GetItemQueryIterator<JObject>(queryDefinition);
		var results = new List<JObject>();

		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		_logger?.LogInformation("Query returned {count} results", results.Count);
		return results;
	}

	public async Task<(IEnumerable<JObject> Results, string ContinuationToken)> QueryWithPaginationAsync(string containerName, string sql, int maxItemCount, string continuationToken = null)
	{
		_logger?.LogInformation("Executing paginated query on container {containerName}: {sql}", containerName, sql);
		_logger?.LogInformation("Max items: {maxItemCount}, Continuation token: {continuationToken}", maxItemCount, continuationToken ?? "null");

		var container = _cosmosClient.GetContainer(_databaseId, containerName);
		var queryDefinition = new QueryDefinition(sql);

		// Create query options with pagination parameters
		var queryOptions = new QueryRequestOptions
		{
			MaxItemCount = maxItemCount
		};

		// Use the continuation token if provided
		var iterator = string.IsNullOrEmpty(continuationToken)
			? container.GetItemQueryIterator<JObject>(queryDefinition, requestOptions: queryOptions)
			: container.GetItemQueryIterator<JObject>(queryDefinition, continuationToken, requestOptions: queryOptions);

		// Read the next page
		var response = await iterator.ReadNextAsync();

		_logger?.LogInformation("Query returned {count} results with continuation token: {token}",
			response.Count, response.ContinuationToken ?? "null");

		return (response, response.ContinuationToken);
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
}
