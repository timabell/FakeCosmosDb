using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

public class InMemoryCosmosDbMock : ICosmosDbMock
{
    private readonly Dictionary<string, CosmosDbContainer> _containers = new();

    public Task AddContainerAsync(string containerName)
    {
        if (!_containers.ContainsKey(containerName))
            _containers[containerName] = new CosmosDbContainer();
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
        if (!_containers.ContainsKey(containerName))
            throw new InvalidOperationException($"Container '{containerName}' does not exist.");

        return _containers[containerName].QueryAsync(sql);
    }

    public Task<(IEnumerable<JObject> Results, string ContinuationToken)> QueryWithPaginationAsync(string containerName, string sql, int maxItemCount, string continuationToken = null)
    {
        if (!_containers.ContainsKey(containerName))
            throw new InvalidOperationException($"Container '{containerName}' does not exist.");

        return _containers[containerName].QueryWithPaginationAsync(sql, maxItemCount, continuationToken);
    }
}
