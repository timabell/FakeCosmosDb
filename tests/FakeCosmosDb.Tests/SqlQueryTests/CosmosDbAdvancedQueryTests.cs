using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using TimAbell.FakeCosmosDb.Tests.Utilities;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests.SqlQueryTests;

public class CosmosDbAdvancedQueryTests
{
	private readonly FakeCosmosDb _cosmosDb;
	private readonly string _databaseName = "AdvancedQueryTestDb";
	private readonly string _containerName = "AdvancedQueryTestContainer";
	private readonly ITestOutputHelper _output;

	public CosmosDbAdvancedQueryTests(ITestOutputHelper output)
	{
		_output = output;
		_cosmosDb = new FakeCosmosDb();
	}

	private async Task InitializeAsync()
	{
		await _cosmosDb.CreateDatabaseIfNotExistsAsync(_databaseName);
		// Container is already created by GetContainer, no need for CreateIfNotExistsAsync
	}

	private async Task AddTestItemAsync<T>(T item)
	{
		var container = _cosmosDb.GetContainer(_databaseName, _containerName);
		await container.CreateItemAsync(item);
	}

	private async Task SeedTestData()
	{
		// Add test items with various properties for testing different query functions
		await AddTestItemAsync(new
		{
			id = "1",
			Name = "John Smith",
			Email = "john.smith@example.com",
			Age = 30,
			Address = new { City = "New York", ZipCode = "10001" }
		});

		await AddTestItemAsync(new
		{
			id = "2",
			Name = "Jane Doe",
			Email = "jane.doe@example.com",
			Age = 25,
			Address = new { City = "Los Angeles", ZipCode = "90001" }
		});

		await AddTestItemAsync(new
		{
			id = "3",
			Name = "Bob Johnson",
			Email = "bob.johnson@example.com",
			Age = 40,
			Address = new { City = "Chicago", ZipCode = "60601" }
		});

		await AddTestItemAsync(new
		{
			id = "4",
			Name = "Alice Brown",
			Email = "alice.brown@example.com",
			Age = 35,
			Address = new { City = "San Francisco", ZipCode = "94105" }
		});
	}

	[Fact]
	public async Task Query_WithContains_ReturnsCorrectItems()
	{
		await InitializeAsync();
		await SeedTestData();

		// Query using CONTAINS function
		var container = _cosmosDb.GetContainer(_databaseName, _containerName);
		var queryDefinition = new QueryDefinition("SELECT * FROM c WHERE CONTAINS(c.Name, 'oh')");
		var iterator = container.GetItemQueryIterator<JObject>(queryDefinition);
		var response = await iterator.ReadNextAsync();

		// Should match "John Smith" and "Bob Johnson"
		Assert.Equal(2, response.Count);
		Assert.Contains(response, item => item["Name"].ToString() == "John Smith");
		Assert.Contains(response, item => item["Name"].ToString() == "Bob Johnson");
	}

	[Fact]
	public async Task Query_WithStartsWith_ReturnsCorrectItems()
	{
		await InitializeAsync();
		await SeedTestData();

		// Query using STARTSWITH function
		var container = _cosmosDb.GetContainer(_databaseName, _containerName);
		var queryDefinition = new QueryDefinition("SELECT * FROM c WHERE STARTSWITH(c.Name, 'J')");
		var iterator = container.GetItemQueryIterator<JObject>(queryDefinition);
		var response = await iterator.ReadNextAsync();

		// Should match "John Smith" and "Jane Doe"
		Assert.Equal(2, response.Count);
		Assert.Contains(response, item => item["Name"].ToString() == "John Smith");
		Assert.Contains(response, item => item["Name"].ToString() == "Jane Doe");
	}

	[Fact]
	public async Task Query_WithGreaterThan_ReturnsCorrectItems()
	{
		await InitializeAsync();
		await SeedTestData();

		// Query using greater than comparison
		var container = _cosmosDb.GetContainer(_databaseName, _containerName);
		var queryDefinition = new QueryDefinition("SELECT * FROM c WHERE c.Age > 30");
		var iterator = container.GetItemQueryIterator<JObject>(queryDefinition);
		var response = await iterator.ReadNextAsync();

		// Should match "Bob Johnson" (40) and "Alice Brown" (35)
		Assert.Equal(2, response.Count);
		Assert.Contains(response, item => item["Name"].ToString() == "Bob Johnson");
		Assert.Contains(response, item => item["Name"].ToString() == "Alice Brown");
	}

	[Fact]
	public async Task Query_WithLessThan_ReturnsCorrectItems()
	{
		await InitializeAsync();
		await SeedTestData();

		// Query using less than comparison
		var container = _cosmosDb.GetContainer(_databaseName, _containerName);
		var queryDefinition = new QueryDefinition("SELECT * FROM c WHERE c.Age < 30");
		var iterator = container.GetItemQueryIterator<JObject>(queryDefinition);
		var response = await iterator.ReadNextAsync();

		// Should match "Jane Doe" (25)
		Assert.Single(response);
		Assert.Equal("Jane Doe", response.First()["Name"].ToString());
	}

	[Fact]
	public async Task Query_WithNestedProperty_ReturnsCorrectItems()
	{
		await InitializeAsync();
		await SeedTestData();

		// Query using a nested property
		var container = _cosmosDb.GetContainer(_databaseName, _containerName);
		var queryDefinition = new QueryDefinition("SELECT * FROM c WHERE c.Address.City = 'Chicago'");
		var iterator = container.GetItemQueryIterator<JObject>(queryDefinition);
		var response = await iterator.ReadNextAsync();

		// Should match "Bob Johnson"
		Assert.Single(response);
		Assert.Equal("Bob Johnson", response.First()["Name"].ToString());
	}
}
