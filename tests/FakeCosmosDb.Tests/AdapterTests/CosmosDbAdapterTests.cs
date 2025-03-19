using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using TimAbell.FakeCosmosDb.Tests.Utilities;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests.AdapterTests;

public class CosmosDbAdapterTests
{
	private readonly string _containerName = "AdapterTest";
	private readonly string _mockConnectionString = "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
	private readonly ITestOutputHelper _output;
	private FakeCosmosDb _cosmosDb;
	private Container _container;
	private readonly string _databaseName = "TestDatabase";

	public CosmosDbAdapterTests(ITestOutputHelper output)
	{
		_output = output;
	}

	private async Task InitializeAsync()
	{
		_cosmosDb = new FakeCosmosDb();
		await _cosmosDb.CreateDatabaseIfNotExistsAsync(_databaseName);
		_container = _cosmosDb.GetContainer(_databaseName, _containerName);
	}

	private async Task AddTestItemAsync<T>(T item)
	{
		await _container.CreateItemAsync(item);
	}

	// todo: Add tests for the CosmosDbAdapter
	[Fact(Skip = "Requires CosmosDB Emulator")]
	public async Task Adapter_CanBeUsedWithCosmosDbInterface()
	{
		// Arrange
		await InitializeAsync();
		var logger = new TestLogger(_output);
		var clientOptions = new CosmosClientOptions();
		var adapter = new CosmosDbAdapter(_mockConnectionString, clientOptions, logger);

		// Act - Add an item
		var testItem = new { id = "test-id", Name = "Test Item", Value = 42 };
		await AddTestItemAsync(testItem);

		// Act - Query the item
		var adapterContainer = adapter.GetContainer(_databaseName, _containerName);
		var queryDefinition = new QueryDefinition("SELECT * FROM c WHERE c.id = 'test-id'");
		var iterator = adapterContainer.GetItemQueryIterator<JObject>(queryDefinition);
		var response = await iterator.ReadNextAsync();

		// Assert
		Assert.Single(response);
		Assert.Equal("Test Item", response.First()["Name"].ToString());
		Assert.Equal(42, (int)response.First()["Value"]);
	}

	[Fact(Skip = "Requires CosmosDB Emulator")]
	public async Task Adapter_SupportsPagination()
	{
		// Arrange
		await InitializeAsync();
		var logger = new TestLogger(_output);
		var clientOptions = new CosmosClientOptions();
		var adapter = new CosmosDbAdapter(_mockConnectionString, clientOptions, logger);

		// Add 20 items
		for (int i = 1; i <= 20; i++)
		{
			await AddTestItemAsync(new
			{
				id = i.ToString(),
				Name = $"Item {i}",
				Value = i
			});
		}

		// Act - Query with pagination (5 items per page)
		var adapterContainer = adapter.GetContainer(_databaseName, _containerName);
		var queryDefinition = new QueryDefinition("SELECT * FROM c");
		var requestOptions = new QueryRequestOptions { MaxItemCount = 5 };

		// First page
		var page1Iterator = adapterContainer.GetItemQueryIterator<JObject>(queryDefinition, null, requestOptions);
		var page1Response = await page1Iterator.ReadNextAsync();
		var page1Results = page1Response.ToList();
		var page1Token = page1Iterator.HasMoreResults ? page1Response.ContinuationToken : null;

		// Assert first page
		Assert.Equal(5, page1Results.Count);
		Assert.NotNull(page1Token);

		// Act - Get second page
		var page2Iterator = adapterContainer.GetItemQueryIterator<JObject>(queryDefinition, page1Token, requestOptions);
		var page2Response = await page2Iterator.ReadNextAsync();
		var page2Results = page2Response.ToList();
		var page2Token = page2Iterator.HasMoreResults ? page2Response.ContinuationToken : null;

		// Assert second page
		Assert.Equal(5, page2Results.Count);
		Assert.NotNull(page2Token);

		// Verify no overlap between pages
		var page1Ids = page1Results.Select(item => item["id"].ToString()).ToList();
		var page2Ids = page2Results.Select(item => item["id"].ToString()).ToList();
		Assert.Empty(page1Ids.Intersect(page2Ids));
	}

	[Fact(Skip = "Requires CosmosDB Emulator")]
	public async Task Adapter_HandlesAdvancedQueries()
	{
		// Arrange
		await InitializeAsync();
		var logger = new TestLogger(_output);
		var clientOptions = new CosmosClientOptions();
		var adapter = new CosmosDbAdapter(_mockConnectionString, clientOptions, logger);

		// Add test items
		await AddTestItemAsync(new
		{
			id = "1",
			Name = "Product A",
			Category = "Electronics",
			Price = 299.99
		});

		await AddTestItemAsync(new
		{
			id = "2",
			Name = "Product B",
			Category = "Books",
			Price = 19.99
		});

		await AddTestItemAsync(new
		{
			id = "3",
			Name = "Product C",
			Category = "Electronics",
			Price = 149.99
		});

		var adapterContainer = adapter.GetContainer(_databaseName, _containerName);

		// Act - Query with CONTAINS
		var containsQueryDefinition = new QueryDefinition("SELECT * FROM c WHERE CONTAINS(c.Name, 'Product')");
		var containsIterator = adapterContainer.GetItemQueryIterator<JObject>(containsQueryDefinition);
		var containsResponse = await containsIterator.ReadNextAsync();
		var containsResults = containsResponse.ToList();

		// Assert
		Assert.Equal(3, containsResults.Count);

		// Act - Query with comparison and projection
		var projectionQueryDefinition = new QueryDefinition("SELECT c.Name, c.Price FROM c WHERE c.Price < 100");
		var projectionIterator = adapterContainer.GetItemQueryIterator<JObject>(projectionQueryDefinition);
		var projectionResponse = await projectionIterator.ReadNextAsync();
		var projectionResults = projectionResponse.ToList();

		// Assert
		Assert.Single(projectionResults);
		Assert.Equal("Product B", projectionResults.First()["Name"].ToString());
		Assert.Equal(19.99, (double)projectionResults.First()["Price"]);

		// Verify projection only returned requested fields (plus id)
		Assert.Equal(3, projectionResults.First().Properties().Count()); // id, Name, Price
	}
}
