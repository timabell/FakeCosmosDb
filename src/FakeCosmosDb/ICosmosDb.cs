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
	Task AddContainerAsync(string containerName);
	Task AddItemAsync(string containerName, object entity);
	Task<IEnumerable<JObject>> QueryAsync(string containerName, string sql);
	Task<(IEnumerable<JObject> Results, string ContinuationToken)> QueryWithPaginationAsync(string containerName, string sql, int maxItemCount, string continuationToken = null);
	Container GetContainer(string databaseName, string containerId);
	Task<DatabaseResponse> CreateDatabaseIfNotExistsAsync(string databaseName, int? throughput = null, RequestOptions requestOptions = null, CancellationToken cancellationToken = default);
	Database GetDatabase(string databaseName);
}
