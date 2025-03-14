using System.Linq;
using System.Threading.Tasks;
using InMemoryCosmosDbMock.Tests.Utilities;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.MockableCosmos.Tests;

public class CosmosDbAdapterTests
{
	private readonly string _containerName = "AdapterTest";
	private readonly string _mockConnectionString = "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
	private readonly ITestOutputHelper _output;

	public CosmosDbAdapterTests(ITestOutputHelper output)
	{
		_output = output;
	}

	// Skip these tests when running in CI since they require the CosmosDB Emulator
	[Fact(Skip = "Requires CosmosDB Emulator")]
	public async Task Adapter_CanBeUsedWithCosmosDbInterface()
	{
		// Arrange
		var logger = new TestLogger(_output);
		var clientOptions = new CosmosClientOptions();
		var mockAdapter = new CosmosDbAdapter(_mockConnectionString, clientOptions, logger);
		await mockAdapter.AddContainerAsync(_containerName);

		// Act - Add an item
		var testItem = new { id = "test-id", Name = "Test Item", Value = 42 };
		await mockAdapter.AddItemAsync(_containerName, testItem);

		// Act - Query the item
		var results = await mockAdapter.QueryAsync(_containerName, "SELECT * FROM c WHERE c.id = 'test-id'");

		// Assert
		Assert.Single(results);
		Assert.Equal("Test Item", results.First()["Name"].ToString());
		Assert.Equal(42, (int)results.First()["Value"]);
	}

	[Fact(Skip = "Requires CosmosDB Emulator")]
	public async Task Adapter_SupportsPagination()
	{
		// Arrange
		var logger = new TestLogger(_output);
		var clientOptions = new CosmosClientOptions();
		var mockAdapter = new CosmosDbAdapter(_mockConnectionString, clientOptions, logger);
		await mockAdapter.AddContainerAsync(_containerName);

		// Add 20 items
		for (int i = 1; i <= 20; i++)
		{
			await mockAdapter.AddItemAsync(_containerName, new
			{
				id = i.ToString(),
				Name = $"Item {i}",
				Value = i
			});
		}

		// Act - Query with pagination (5 items per page)
		var (page1Results, page1Token) = await mockAdapter.QueryWithPaginationAsync(_containerName, "SELECT * FROM c", 5);

		// Assert first page
		Assert.Equal(5, page1Results.Count());
		Assert.NotNull(page1Token);

		// Act - Get second page
		var (page2Results, page2Token) = await mockAdapter.QueryWithPaginationAsync(_containerName, "SELECT * FROM c", 5, page1Token);

		// Assert second page
		Assert.Equal(5, page2Results.Count());
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
		var logger = new TestLogger(_output);
		var clientOptions = new CosmosClientOptions();
		var mockAdapter = new CosmosDbAdapter(_mockConnectionString, clientOptions, logger);
		await mockAdapter.AddContainerAsync(_containerName);

		// Add test items
		await mockAdapter.AddItemAsync(_containerName, new
		{
			id = "1",
			Name = "Product A",
			Category = "Electronics",
			Price = 299.99
		});

		await mockAdapter.AddItemAsync(_containerName, new
		{
			id = "2",
			Name = "Product B",
			Category = "Books",
			Price = 19.99
		});

		await mockAdapter.AddItemAsync(_containerName, new
		{
			id = "3",
			Name = "Product C",
			Category = "Electronics",
			Price = 149.99
		});

		// Act - Query with CONTAINS
		var containsResults = await mockAdapter.QueryAsync(_containerName, "SELECT * FROM c WHERE CONTAINS(c.Name, 'Product')");

		// Assert
		Assert.Equal(3, containsResults.Count());

		// Act - Query with comparison and projection
		var projectionResults = await mockAdapter.QueryAsync(_containerName, "SELECT c.Name, c.Price FROM c WHERE c.Price < 100");

		// Assert
		Assert.Single(projectionResults);
		Assert.Equal("Product B", projectionResults.First()["Name"].ToString());
		Assert.Equal(19.99, (double)projectionResults.First()["Price"]);

		// Verify projection only returned requested fields (plus id)
		Assert.Equal(3, projectionResults.First().Properties().Count()); // id, Name, Price
	}
}
