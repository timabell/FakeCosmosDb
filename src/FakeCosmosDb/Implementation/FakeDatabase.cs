using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using Microsoft.Extensions.Logging;

namespace TimAbell.FakeCosmosDb.Implementation;

public class FakeDatabase(string databaseName, ILogger logger = null) : Database
{
	public override string Id => databaseName;
	public override CosmosClient Client => throw new System.NotImplementedException();

	// Dictionary to store containers in this database, keyed by container ID
	private readonly Dictionary<string, FakeContainer> _containers = new();

	// Get or create a container in this database
	public FakeContainer GetOrCreateContainer(string containerId, string partitionKeyPath = "/id")
	{
		if (_containers.TryGetValue(containerId, out var existingContainer))
		{
			return existingContainer;
		}

		var newContainer = new FakeContainer(logger)
		{
			PartitionKeyPath = partitionKeyPath
		};
		_containers[containerId] = newContainer;
		return newContainer;
	}

	public override Task<DatabaseResponse> ReadAsync(RequestOptions requestOptions = null, CancellationToken cancellationToken = new())
	{
		throw new System.NotImplementedException();
	}

	public override Task<DatabaseResponse> DeleteAsync(RequestOptions requestOptions = null, CancellationToken cancellationToken = new())
	{
		throw new System.NotImplementedException();
	}

	public override Task<int?> ReadThroughputAsync(CancellationToken cancellationToken = new())
	{
		throw new System.NotImplementedException();
	}

	public override Task<ThroughputResponse> ReadThroughputAsync(RequestOptions requestOptions, CancellationToken cancellationToken = new())
	{
		throw new System.NotImplementedException();
	}

	public override Task<ThroughputResponse> ReplaceThroughputAsync(ThroughputProperties throughputProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = new())
	{
		throw new System.NotImplementedException();
	}

	public override Task<ContainerResponse> CreateContainerAsync(ContainerProperties containerProperties, ThroughputProperties throughputProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = new())
	{
		throw new System.NotImplementedException();
	}

	public override Task<ContainerResponse> CreateContainerIfNotExistsAsync(ContainerProperties containerProperties, ThroughputProperties throughputProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = new())
	{
		throw new System.NotImplementedException();
	}

	public override Task<ResponseMessage> CreateContainerStreamAsync(ContainerProperties containerProperties, ThroughputProperties throughputProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = new())
	{
		throw new System.NotImplementedException();
	}

	public override Task<ThroughputResponse> ReplaceThroughputAsync(int throughput, RequestOptions requestOptions = null, CancellationToken cancellationToken = new())
	{
		throw new System.NotImplementedException();
	}

	public override Task<ResponseMessage> ReadStreamAsync(RequestOptions requestOptions = null, CancellationToken cancellationToken = new())
	{
		throw new System.NotImplementedException();
	}

	public override Task<ResponseMessage> DeleteStreamAsync(RequestOptions requestOptions = null, CancellationToken cancellationToken = new())
	{
		throw new System.NotImplementedException();
	}

	public override Container GetContainer(string id)
	{
		return GetOrCreateContainer(id);
	}

	public override Task<ContainerResponse> CreateContainerAsync(ContainerProperties containerProperties, int? throughput = null, RequestOptions requestOptions = null, CancellationToken cancellationToken = new())
	{
		var container = new FakeContainer();
		_containers[containerProperties.Id] = container;
		var response = new FakeContainerResponse(container);
		return Task.FromResult<ContainerResponse>(response);
	}

	public override Task<ContainerResponse> CreateContainerAsync(string id, string partitionKeyPath, int? throughput = null, RequestOptions requestOptions = null, CancellationToken cancellationToken = new())
	{
		throw new System.NotImplementedException();
	}

	public override async Task<ContainerResponse> CreateContainerIfNotExistsAsync(ContainerProperties containerProperties, int? throughput = null, RequestOptions requestOptions = null, CancellationToken cancellationToken = new())
	{
		if (!_containers.TryGetValue(containerProperties.Id, out var container))
		{
			return await CreateContainerAsync(containerProperties, throughput, requestOptions, cancellationToken);
		}
		return new FakeContainerResponse(container);
	}

	public override Task<ContainerResponse> CreateContainerIfNotExistsAsync(string id, string partitionKeyPath, int? throughput = null, RequestOptions requestOptions = null, CancellationToken cancellationToken = new())
	{
		throw new System.NotImplementedException();
	}

	public override Task<ResponseMessage> CreateContainerStreamAsync(ContainerProperties containerProperties, int? throughput = null, RequestOptions requestOptions = null, CancellationToken cancellationToken = new())
	{
		throw new System.NotImplementedException();
	}

	public override User GetUser(string id)
	{
		throw new System.NotImplementedException();
	}

	public override Task<UserResponse> CreateUserAsync(string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = new())
	{
		throw new System.NotImplementedException();
	}

	public override Task<UserResponse> UpsertUserAsync(string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = new())
	{
		throw new System.NotImplementedException();
	}

	public override FeedIterator<T> GetContainerQueryIterator<T>(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		throw new System.NotImplementedException();
	}

	public override FeedIterator GetContainerQueryStreamIterator(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		throw new System.NotImplementedException();
	}

	public override FeedIterator<T> GetContainerQueryIterator<T>(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		throw new System.NotImplementedException();
	}

	public override FeedIterator GetContainerQueryStreamIterator(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		throw new System.NotImplementedException();
	}

	public override FeedIterator<T> GetUserQueryIterator<T>(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		throw new System.NotImplementedException();
	}

	public override FeedIterator<T> GetUserQueryIterator<T>(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		throw new System.NotImplementedException();
	}

	public override ContainerBuilder DefineContainer(string name, string partitionKeyPath)
	{
		throw new System.NotImplementedException();
	}

	public override ClientEncryptionKey GetClientEncryptionKey(string id)
	{
		throw new System.NotImplementedException();
	}

	public override FeedIterator<ClientEncryptionKeyProperties> GetClientEncryptionKeyQueryIterator(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		throw new System.NotImplementedException();
	}

	public override Task<ClientEncryptionKeyResponse> CreateClientEncryptionKeyAsync(ClientEncryptionKeyProperties clientEncryptionKeyProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = new())
	{
		throw new System.NotImplementedException();
	}
}
