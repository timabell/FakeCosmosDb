using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using InMemoryCosmosDbMock.Tests.Utilities;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.MockableCosmos.Tests;

public class CosmosDbIndexTests
{
	private readonly CosmosInMemoryCosmosDb _db;
	private readonly string _containerName = "IndexTest";
	private readonly ITestOutputHelper _output;

	public CosmosDbIndexTests(ITestOutputHelper output)
	{
		_output = output;
		_db = new CosmosInMemoryCosmosDb(new TestLogger(output));
		_db.AddContainerAsync(_containerName).Wait();
	}

	[Fact]
	public async Task Index_ImproveQueryPerformance()
	{
		// Add a large number of items to test indexing performance
		const int itemCount = 1000;
		await SeedLargeDataset(itemCount);

		// Measure performance of a query that should use the index
		var stopwatch = new Stopwatch();
		stopwatch.Start();

		var results = await _db.QueryAsync(_containerName, "SELECT * FROM c WHERE c.Category = 'A'");

		stopwatch.Stop();
		var queryTime = stopwatch.ElapsedMilliseconds;

		// Verify results
		Assert.Equal(itemCount / 3, results.Count()); // Category A should be 1/3 of all items

		// Performance assertion - this is a soft assertion as performance can vary
		// but indexed queries should be reasonably fast even with large datasets
		Assert.True(queryTime < 500, $"Query took {queryTime}ms which is longer than expected for an indexed query");
	}

	[Fact]
	public async Task Index_SupportsMultipleFields()
	{
		// Add test items with multiple indexed fields
		await SeedTestDataWithMultipleFields();

		// Query by one indexed field
		var resultsByName = await _db.QueryAsync(_containerName, "SELECT * FROM c WHERE c.Name = 'John'");
		Assert.Single(resultsByName);

		// Query by another indexed field
		var resultsByCity = await _db.QueryAsync(_containerName, "SELECT * FROM c WHERE c.City = 'New York'");
		Assert.Single(resultsByCity);

		// Query by a third indexed field
		var resultsByAge = await _db.QueryAsync(_containerName, "SELECT * FROM c WHERE c.Age = 30");
		Assert.Single(resultsByAge);
	}

	private async Task SeedLargeDataset(int count)
	{
		for (int i = 1; i <= count; i++)
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

	private async Task SeedTestDataWithMultipleFields()
	{
		await _db.AddItemAsync(_containerName, new
		{
			id = "1",
			Name = "John",
			City = "New York",
			Age = 30
		});

		await _db.AddItemAsync(_containerName, new
		{
			id = "2",
			Name = "Jane",
			City = "Los Angeles",
			Age = 25
		});

		await _db.AddItemAsync(_containerName, new
		{
			id = "3",
			Name = "Bob",
			City = "Chicago",
			Age = 40
		});
	}
}
