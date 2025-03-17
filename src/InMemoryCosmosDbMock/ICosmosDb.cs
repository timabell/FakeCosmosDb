using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;

namespace TimAbell.MockableCosmos;

public interface ICosmosDb
{
	Task AddContainerAsync(string containerName);
	Task AddItemAsync(string containerName, object entity);
	Task<IEnumerable<JObject>> QueryAsync(string containerName, string sql);
	Task<(IEnumerable<JObject> Results, string ContinuationToken)> QueryWithPaginationAsync(string containerName, string sql, int maxItemCount, string continuationToken = null);
	Container GetContainer(string databaseName, string containerId);
	Task CreateDatabaseIfNotExistsAsync(string databaseName);
	Database GetDatabase(string databaseName);
}
