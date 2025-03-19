using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using TimAbell.FakeCosmosDb.Tests.Utilities;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests.SqlQueryTests;

public class TopClauseTests
{
	private readonly FakeCosmosDb _cosmosDb;
	private readonly string _databaseName = "TopClauseTestDb";
	private readonly string _containerName = "TopClauseTestContainer";
	private readonly ITestOutputHelper _output;

	public TopClauseTests(ITestOutputHelper output)
	{
		_output = output;
		_cosmosDb = new FakeCosmosDb();
	}

	private async Task<Container> InitializeContainerAsync()
	{
		await _cosmosDb.CreateDatabaseIfNotExistsAsync(_databaseName);
		return _cosmosDb.GetContainer(_databaseName, _containerName);
	}

	public class TestItem
	{
		public string Id { get; set; }
		public string Name { get; set; }
		public int Value { get; set; }
	}

	[Fact]
	public async Task TopWithFewerItemsThanLimit_ShouldReturnAllItems()
	{
		// Arrange
		var container = await InitializeContainerAsync();

		// Create test items - only 5 items (less than TOP 10)
		var testItems = new List<TestItem>();
		for (int i = 1; i <= 5; i++)
		{
			var item = new TestItem
			{
				Id = $"item-{i}",
				Name = $"Test Item {i}",
				Value = i * 10
			};
			testItems.Add(item);
			await container.CreateItemAsync(item);
		}

		_output.WriteLine($"Created {testItems.Count} test items");

		// Act - Query with TOP 10
		var query = new QueryDefinition("SELECT TOP 10 * FROM c");
		var iterator = container.GetItemQueryIterator<TestItem>(query);
		var response = await iterator.ReadNextAsync();
		var results = response.ToList();

		_output.WriteLine($"Query returned {results.Count} items");
		foreach (var item in results)
		{
			_output.WriteLine($"Item: Id={item.Id}, Name={item.Name}, Value={item.Value}");
		}

		// Assert
		Assert.Equal(testItems.Count, results.Count);

		// Verify all items were returned and match the original data
		foreach (var original in testItems)
		{
			var result = results.FirstOrDefault(r => r.Id == original.Id);
			Assert.NotNull(result);
			Assert.Equal(original.Name, result.Name);
			Assert.Equal(original.Value, result.Value);
		}
	}

	[Fact]
	public async Task TopWithFewerItems_ShouldWorkWithProjection()
	{
		// Arrange
		var container = await InitializeContainerAsync();

		// Create test items - only 3 items (less than TOP 10)
		var testItems = new List<TestItem>();
		for (int i = 1; i <= 3; i++)
		{
			var item = new TestItem
			{
				Id = $"proj-item-{i}",
				Name = $"Projection Item {i}",
				Value = i * 5
			};
			testItems.Add(item);
			await container.CreateItemAsync(item);
		}

		_output.WriteLine($"Created {testItems.Count} test items for projection test");

		// Act - Query with TOP 10 and projection
		var query = new QueryDefinition("SELECT TOP 10 c.Id, c.Name FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();
		var results = response.ToList();

		_output.WriteLine($"Projection query returned {results.Count} items");
		foreach (var item in results)
		{
			_output.WriteLine($"Projected Item: {item}");
		}

		// Assert
		Assert.Equal(testItems.Count, results.Count);

		// Verify all items were returned with correct projection
		foreach (var result in results)
		{
			Assert.NotNull(result["Id"]);
			Assert.NotNull(result["Name"]);
			Assert.Null(result["Value"]); // This should not be included in the projection

			// Find the matching original item
			var originalId = result["Id"].ToString();
			var original = testItems.FirstOrDefault(t => t.Id == originalId);
			Assert.NotNull(original);
			Assert.Equal(original.Name, result["Name"].ToString());
		}
	}
}
