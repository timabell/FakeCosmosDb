using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using TimAbell.FakeCosmosDb.Tests.Utilities;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests.SqlQueryTests;

public class CosmosDbPaginationTests
{
	private readonly FakeCosmosDb _db;
	private readonly string _containerName = "PaginationTest";
	private readonly ITestOutputHelper _output;
	private readonly string _databaseName = "TestDatabase";

	public CosmosDbPaginationTests(ITestOutputHelper output)
	{
		_output = output;
		_db = new FakeCosmosDb(new TestLogger(output));
		// Container is created by GetContainer, no need for AddContainerAsync
		SeedTestData().Wait();
	}

	private async Task SeedTestData()
	{
		var container = _db.GetContainer(_databaseName, _containerName);

		// Add 20 test items
		for (int i = 1; i <= 20; i++)
		{
			await container.CreateItemAsync(new
			{
				id = i.ToString(),
				Name = $"Item {i}",
				Category = i % 3 == 0 ? "A" : (i % 3 == 1 ? "B" : "C"),
				Value = i * 10
			});
		}
	}

	[Fact]
	public async Task Pagination_ReturnsCorrectNumberOfItems()
	{
		var container = _db.GetContainer(_databaseName, _containerName);

		// Request first page with 5 items
		var queryDefinition = new QueryDefinition("SELECT * FROM c");
		var requestOptions = new QueryRequestOptions { MaxItemCount = 5 };
		var iterator = container.GetItemQueryIterator<JObject>(queryDefinition, null, requestOptions);
		var page1Response = await iterator.ReadNextAsync();
		var page1Results = page1Response.ToList();
		var page1Token = iterator.HasMoreResults ? page1Response.ContinuationToken : null;

		// Verify first page
		Assert.Equal(5, page1Results.Count);
		Assert.NotNull(page1Token);

		// Request second page
		var page2Iterator = container.GetItemQueryIterator<JObject>(queryDefinition, page1Token, requestOptions);
		var page2Response = await page2Iterator.ReadNextAsync();
		var page2Results = page2Response.ToList();
		var page2Token = page2Iterator.HasMoreResults ? page2Response.ContinuationToken : null;

		// Verify second page
		Assert.Equal(5, page2Results.Count);
		Assert.NotNull(page2Token);

		// Verify no overlap between pages
		var page1Ids = page1Results.Select(item => item["id"].ToString()).ToList();
		var page2Ids = page2Results.Select(item => item["id"].ToString()).ToList();
		Assert.Empty(page1Ids.Intersect(page2Ids));
	}

	[Fact]
	public async Task Pagination_WithFilter_ReturnsCorrectItems()
	{
		var container = _db.GetContainer(_databaseName, _containerName);

		// Request items from category A with pagination
		var queryDefinition = new QueryDefinition("SELECT * FROM c WHERE c.Category = 'A'");
		var requestOptions = new QueryRequestOptions { MaxItemCount = 3 };
		var iterator = container.GetItemQueryIterator<JObject>(queryDefinition, null, requestOptions);
		var response = await iterator.ReadNextAsync();
		var results = response.ToList();
		var token = iterator.HasMoreResults ? response.ContinuationToken : null;

		// Verify results
		Assert.Equal(3, results.Count);
		foreach (var item in results)
		{
			Assert.Equal("A", item["Category"].ToString());
		}
	}

	[Fact]
	public async Task Pagination_LastPage_HasNoContinuationToken()
	{
		var container = _db.GetContainer(_databaseName, _containerName);

		// Request all items with a large enough page size
		var queryDefinition = new QueryDefinition("SELECT * FROM c");
		var requestOptions = new QueryRequestOptions { MaxItemCount = 25 };
		var iterator = container.GetItemQueryIterator<JObject>(queryDefinition, null, requestOptions);
		var response = await iterator.ReadNextAsync();
		var results = response.ToList();
		var token = iterator.HasMoreResults ? response.ContinuationToken : null;

		// Verify we got all items and no continuation token
		Assert.Equal(20, results.Count);
		Assert.Null(token);
	}
}
