using System.Linq;
using System.Threading.Tasks;
using AwesomeAssertions;
using AwesomeAssertions.Execution;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using TimAbell.FakeCosmosDb.Tests.Utilities;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests.SqlQueryTests;

public class CosmosDbProjectionTests
{
	private readonly FakeCosmosDb _cosmosDb;
	private readonly string _databaseName = "ProjectionTestDb";
	private readonly string _containerName = "ProjectionTestContainer";
	private readonly ITestOutputHelper _output;
	private readonly ILogger _logger;
	private Container _container;

	public CosmosDbProjectionTests(ITestOutputHelper output)
	{
		_output = output;
		_logger = new TestLogger(output);
		_cosmosDb = new FakeCosmosDb(_logger);
		InitializeAsync().Wait();
	}

	private async Task InitializeAsync()
	{
		await _cosmosDb.CreateDatabaseIfNotExistsAsync(_databaseName);
		_container = _cosmosDb.GetContainer(_databaseName, _containerName);
		await SeedTestData();
	}

	private async Task SeedTestData()
	{
		// Add test items with various properties for testing projections
		await AddTestItemAsync(new
		{
			id = "1",
			Name = "John Smith",
			Email = "john.smith@example.com",
			Age = 30,
			Address = new { City = "New York", ZipCode = "10001", Country = "USA" },
			Tags = new[] { "developer", "c#", "azure" }
		});

		await AddTestItemAsync(new
		{
			id = "2",
			Name = "Jane Doe",
			Email = "jane.doe@example.com",
			Age = 25,
			Address = new { City = "Los Angeles", ZipCode = "90001", Country = "USA" },
			Tags = new[] { "designer", "ui", "ux" }
		});

		await AddTestItemAsync(new
		{
			id = "3",
			Name = "Bob Johnson",
			Email = "bob.johnson@example.com",
			Age = 40,
			Address = new { City = "Chicago", ZipCode = "60601", Country = "USA" },
			Tags = new[] { "manager", "agile", "scrum" }
		});
	}

	private async Task AddTestItemAsync<T>(T item)
	{
		await _container.CreateItemAsync(item);
	}

	[Fact]
	public async Task Query_WithSelectProjection_ReturnsOnlyRequestedFields()
	{
		// Query with projection
		var results = await _container.GetItemQueryIterator<Newtonsoft.Json.Linq.JObject>("SELECT c.Name, c.Age FROM c").ReadNextAsync();

		// Should return 3 items with only Name and Age (plus id for consistency)
		Assert.Equal(3, results.Count);

		foreach (var item in results)
		{
			// Verify only requested properties (plus id) are returned
			Assert.Equal(3, item.Properties().Count());
			Assert.Contains("id", item.Properties().Select(p => p.Name));
			Assert.Contains("Name", item.Properties().Select(p => p.Name));
			Assert.Contains("Age", item.Properties().Select(p => p.Name));

			// Verify other properties are not included
			Assert.DoesNotContain("Email", item.Properties().Select(p => p.Name));
			Assert.DoesNotContain("Address", item.Properties().Select(p => p.Name));
			Assert.DoesNotContain("Tags", item.Properties().Select(p => p.Name));
		}
	}

	[Fact]
	public async Task Query_WithNestedPropertyProjection_ReturnsNestedStructure()
	{
		// Query with nested property projection
		var results = await _container.GetItemQueryIterator<Newtonsoft.Json.Linq.JObject>("SELECT c.Name, c.Address.City FROM c").ReadNextAsync();

		// Should return 3 items with Name and Address.City
		results.Should().HaveCount(3, because: "we have 3 documents in the test container");

		// Log all results for debugging
		foreach (var item in results)
		{
			_logger?.LogInformation("Result item: {Item}", item.ToString());
		}

		// Check each result using AssertionScope to see all issues at once
		int index = 0;
		foreach (var item in results)
		{
			using (new AssertionScope($"Item at index {index++}"))
			{
				// Expected properties
				item.Should().ContainKey("id", because: "'id' should always be included");
				item.Should().ContainKey("Name", because: "it was requested in the SELECT clause");
				item.Should().ContainKey("Address", because: "Address.City was requested in the SELECT clause");

				// Verify other properties are NOT included
				item.Should().NotContainKey("Email", because: "it wasn't requested in the projection");
				item.Should().NotContainKey("Age", because: "it wasn't requested in the projection");
				item.Should().NotContainKey("Tags", because: "it wasn't requested in the projection");

				// Check Address structure
				if (item.ContainsKey("Address"))
				{
					var address = item["Address"];
					address.Should().NotBeNull();
					address.Should().BeOfType<Newtonsoft.Json.Linq.JObject>();

					// Use another nested AssertionScope for the Address object
					using (new AssertionScope("Address property"))
					{
						var addressObj = address as Newtonsoft.Json.Linq.JObject;
						addressObj.Should().ContainKey("City", because: "it was requested in the SELECT clause");
						addressObj.Should().NotContainKey("Street", because: "it wasn't requested in the projection");
						addressObj.Should().NotContainKey("ZipCode", because: "it wasn't requested in the projection");
						addressObj.Should().NotContainKey("Country", because: "it wasn't requested in the projection");
					}
				}
			}
		}
	}

	[Fact]
	public async Task Query_WithLimit_ReturnsLimitedResults()
	{
		// Query with LIMIT
		var results = await _container.GetItemQueryIterator<Newtonsoft.Json.Linq.JObject>("SELECT * FROM c LIMIT 2").ReadNextAsync();

		// Should return only 2 items
		Assert.Equal(2, results.Count);
	}

	[Fact]
	public async Task Query_WithProjectionAndFilter_ReturnsFilteredProjection()
	{
		// Query with both projection and filter
		var results = await _container.GetItemQueryIterator<Newtonsoft.Json.Linq.JObject>("SELECT c.Name, c.Age FROM c WHERE c.Age > 25").ReadNextAsync();

		// Should return 2 items (John and Bob) with only Name and Age
		Assert.Equal(2, results.Count);

		foreach (var item in results)
		{
			// Verify only requested properties are returned
			Assert.Equal(3, item.Properties().Count()); // id, Name, Age
			Assert.Contains("id", item.Properties().Select(p => p.Name));
			Assert.Contains("Name", item.Properties().Select(p => p.Name));
			Assert.Contains("Age", item.Properties().Select(p => p.Name));

			// Verify age is > 25
			Assert.True((int)item["Age"] > 25);
		}
	}
}
