using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using TimAbell.FakeCosmosDb.Tests.Utilities;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests;

public class CosmosDbTests
{
	private static ILogger _logger;
	private FakeCosmosDb _cosmosDb;
	private Container _container;
	private const string TestDatabaseName = "TestDatabase";
	private const string TestContainerName = "TestContainer";

	public CosmosDbTests(ITestOutputHelper output)
	{
		_logger = new TestLogger(output);
	}

	private async Task InitializeAsync()
	{
		_cosmosDb = new FakeCosmosDb();
		await _cosmosDb.CreateDatabaseIfNotExistsAsync(TestDatabaseName);
		_container = _cosmosDb.GetContainer(TestDatabaseName, TestContainerName);
	}

	private async Task AddTestItemAsync<T>(T item)
	{
		await _container.CreateItemAsync(item);
	}

	[Fact]
	public async Task Can_Insert_And_Query_Item()
	{
		await InitializeAsync();

		var user = new { id = "1", Name = "Alice", Age = 30 };
		await AddTestItemAsync(user);

		var query = new QueryDefinition("SELECT * FROM c WHERE c.Name = @name").WithParameter("@name", "Alice");
		var iterator = _container.GetItemQueryIterator<object>(query);
		var results = await iterator.ReadNextAsync();

		Assert.Single(results);
		var result = results.First();
		Assert.Equal("Alice", result.GetType().GetProperty("Name").GetValue(result));
	}

	public static IEnumerable<object[]> TestConfigurations()
	{
		yield return new object[] { };
		// Skip the real adapter in normal test runs
		// yield return new object[] { new CosmosDbAdapter("AccountEndpoint=https://localhost:8081;AccountKey=your-key;") };
	}
}
