using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;

namespace TimAbell.FakeCosmosDb;

/// <summary>
/// Optional, you can either have your production code depend on this to decouple from the
/// concrete CosmosClient, or you can depend on the concrete CosmosClient and FakeCosmosDb
/// will be a drop-in replacement as it inherits from CosmosClient.
/// Or you can bring your own interface and adapters.
/// </summary>
public interface ICosmosDb
{
	Task<ResponseMessage> CreateDatabaseStreamAsync(DatabaseProperties databaseProperties, int? throughput = null, RequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken());
	FeedIterator<T> GetDatabaseQueryIterator<T>(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null);
	FeedIterator<T> GetDatabaseQueryIterator<T>(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null);
	FeedIterator GetDatabaseQueryStreamIterator(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null);
	Task<AccountProperties> ReadAccountAsync();
	FeedIterator GetDatabaseQueryStreamIterator(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null);
	Task<DatabaseResponse> CreateDatabaseIfNotExistsAsync(string id, ThroughputProperties throughputProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken());
	Container GetContainer(string databaseName, string containerId);
	Task<DatabaseResponse> CreateDatabaseIfNotExistsAsync(string databaseName, int? throughput = null, RequestOptions requestOptions = null, CancellationToken cancellationToken = default);
	Database GetDatabase(string databaseName);
	Task<DatabaseResponse> CreateDatabaseAsync(string id, ThroughputProperties throughputProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken());
	Task<DatabaseResponse> CreateDatabaseAsync(string databaseName, int? throughput = null, RequestOptions requestOptions = null, CancellationToken cancellationToken = default);
}
