using System.Linq;
using System.Threading.Tasks;
using TimAbell.FakeCosmosDb.Tests.Utilities;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests;

public class CosmosDbPaginationTests
{
	private readonly CosmosInMemoryCosmosDb _db;
	private readonly string _containerName = "PaginationTest";
	private readonly ITestOutputHelper _output;

	public CosmosDbPaginationTests(ITestOutputHelper output)
	{
		_output = output;
		_db = new CosmosInMemoryCosmosDb(new TestLogger(output));
		_db.AddContainerAsync(_containerName).Wait();
		SeedTestData().Wait();
	}

	private async Task SeedTestData()
	{
		// Add 20 test items
		for (int i = 1; i <= 20; i++)
		{
			await _db.AddItemAsync(_containerName, new
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
		// Request first page with 5 items
		var (page1Results, page1Token) = await _db.QueryWithPaginationAsync(_containerName, "SELECT * FROM c", 5);

		// Verify first page
		Assert.Equal(5, page1Results.Count());
		Assert.NotNull(page1Token);

		// Request second page
		var (page2Results, page2Token) = await _db.QueryWithPaginationAsync(_containerName, "SELECT * FROM c", 5, page1Token);

		// Verify second page
		Assert.Equal(5, page2Results.Count());
		Assert.NotNull(page2Token);

		// Verify no overlap between pages
		var page1Ids = page1Results.Select(item => item["id"].ToString()).ToList();
		var page2Ids = page2Results.Select(item => item["id"].ToString()).ToList();
		Assert.Empty(page1Ids.Intersect(page2Ids));
	}

	[Fact]
	public async Task Pagination_WithFilter_ReturnsCorrectItems()
	{
		// Request items from category A with pagination
		var (results, token) = await _db.QueryWithPaginationAsync(_containerName, "SELECT * FROM c WHERE c.Category = 'A'", 3);

		// Verify results
		Assert.Equal(3, results.Count());
		foreach (var item in results)
		{
			Assert.Equal("A", item["Category"].ToString());
		}
	}

	[Fact]
	public async Task Pagination_LastPage_HasNoContinuationToken()
	{
		// Request all items with a large enough page size
		var (results, token) = await _db.QueryWithPaginationAsync(_containerName, "SELECT * FROM c", 25);

		// Verify we got all items and no continuation token
		Assert.Equal(20, results.Count());
		Assert.Null(token);
	}
}
