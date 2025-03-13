// Adapter for real CosmosDB
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

public class CosmosDbMockAdapter : ICosmosDbMock
{
    private readonly CosmosClient _cosmosClient;
    private readonly string _databaseId;

    public CosmosDbMockAdapter(string connectionString, string databaseId = "TestDb")
    {
        _cosmosClient = new CosmosClient(connectionString);
        _databaseId = databaseId;
    }

    public async Task AddContainerAsync(string containerName)
    {
        await _cosmosClient.GetDatabase(_databaseId).CreateContainerIfNotExistsAsync(containerName, "/id");
    }

    public async Task AddItemAsync(string containerName, object entity)
    {
        var container = _cosmosClient.GetContainer(_databaseId, containerName);
        await container.CreateItemAsync(entity);
    }

    public async Task<IEnumerable<JObject>> QueryAsync(string containerName, string sql)
    {
        var container = _cosmosClient.GetContainer(_databaseId, containerName);
        var queryDefinition = new QueryDefinition(sql);
        var iterator = container.GetItemQueryIterator<JObject>(queryDefinition);
        var results = new List<JObject>();

        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }
        
        return results;
    }

    public async Task<(IEnumerable<JObject> Results, string ContinuationToken)> QueryWithPaginationAsync(string containerName, string sql, int maxItemCount, string continuationToken = null)
    {
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
        
        return (response, response.ContinuationToken);
    }
}
