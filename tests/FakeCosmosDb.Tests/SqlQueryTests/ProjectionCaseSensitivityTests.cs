using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using TimAbell.FakeCosmosDb.Tests.Utilities;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests.SqlQueryTests;

public class ProjectionCaseSensitivityTests
{
	private readonly FakeCosmosDb _cosmosDb;
	private readonly string _databaseName = "ProjectionCaseTestDb";
	private readonly string _containerName = "ProjectionCaseTestContainer";
	private readonly ITestOutputHelper _output;

	public ProjectionCaseSensitivityTests(ITestOutputHelper output)
	{
		_output = output;
		_cosmosDb = new FakeCosmosDb();
	}

	private async Task InitializeAsync()
	{
		await _cosmosDb.CreateDatabaseIfNotExistsAsync(_databaseName);
	}

	public class TestItem
	{
		public string Id { get; set; }
		public string Name { get; set; }
		public int Age { get; set; }
		public string Email { get; set; }
	}

	[Fact]
	public async Task ProjectionWithDifferentCase_ShouldDeserializeCorrectly()
	{
		// Arrange
		await InitializeAsync();
		var container = _cosmosDb.GetContainer(_databaseName, _containerName);

		// Add test item
		var originalItem = new TestItem
		{
			Id = "test-id-1",
			Name = "John Doe",
			Age = 30,
			Email = "john.doe@example.com"
		};

		await container.CreateItemAsync(originalItem);

		// Act - First query with exactly matching property names (control test)
		var queryExactCase = new QueryDefinition("SELECT c.Id, c.Name, c.Age, c.Email FROM c WHERE c.Id = 'test-id-1'");
		var iteratorExactCase = container.GetItemQueryIterator<TestItem>(queryExactCase);
		var resultExactCase = await iteratorExactCase.ReadNextAsync();

		_output.WriteLine("CONTROL TEST (EXACT CASE):");
		_output.WriteLine($"Result count: {resultExactCase.Count}");
		if (resultExactCase.Any())
		{
			var item = resultExactCase.First();
			_output.WriteLine($"Item values: Id={item.Id}, Name={item.Name}, Age={item.Age}, Email={item.Email}");
		}

		// The control test with exact case should work
		Assert.Single(resultExactCase);

		// Act - Now with lowercase property names
		var queryLowerCaseFields = new QueryDefinition("SELECT c.id, c.name, c.age, c.email FROM c WHERE c.Id = 'test-id-1'");
		var iteratorLowerCaseFields = container.GetItemQueryIterator<TestItem>(queryLowerCaseFields);
		var resultLowerCaseFields = await iteratorLowerCaseFields.ReadNextAsync();

		_output.WriteLine("\nTEST WITH LOWERCASE FIELDS:");
		_output.WriteLine($"Result count: {resultLowerCaseFields.Count}");
		if (resultLowerCaseFields.Any())
		{
			var item = resultLowerCaseFields.First();
			_output.WriteLine($"Item values: Id={item.Id}, Name={item.Name}, Age={item.Age}, Email={item.Email}");
		}

		// Act - Now with lowercase property names
		var queryLowerCase = new QueryDefinition("SELECT c.id, c.name, c.age, c.email FROM c WHERE c.id = 'test-id-1'");
		var iteratorLowerCase = container.GetItemQueryIterator<TestItem>(queryLowerCase);
		var resultLowerCase = await iteratorLowerCase.ReadNextAsync();

		_output.WriteLine("\nTEST WITH LOWERCASE WHERE & PROPERTIES:");
		_output.WriteLine($"Result count: {resultLowerCase.Count}");
		if (resultLowerCase.Any())
		{
			var item = resultLowerCase.First();
			_output.WriteLine($"Item values: Id={item.Id}, Name={item.Name}, Age={item.Age}, Email={item.Email}");
		}

		// Assert - If case insensitive, this should pass
		Assert.Single(resultLowerCase);
		if (resultLowerCase.Any())
		{
			var itemLowerCase = resultLowerCase.First();
			Assert.Equal(originalItem.Id, itemLowerCase.Id);
			Assert.Equal(originalItem.Name, itemLowerCase.Name);
			Assert.Equal(originalItem.Age, itemLowerCase.Age);
			Assert.Equal(originalItem.Email, itemLowerCase.Email);
		}
	}

	[Fact]
	public async Task ProjectionWithMixedCase_ShouldDeserializeCorrectly()
	{
		// Arrange
		await InitializeAsync();
		var container = _cosmosDb.GetContainer(_databaseName, _containerName);

		// Add test item
		var originalItem = new TestItem
		{
			Id = "test-id-2",
			Name = "Jane Smith",
			Age = 25,
			Email = "jane.smith@example.com"
		};

		await container.CreateItemAsync(originalItem);

		// Act - Use projection with mixed casing (but without AS which parser doesn't support)
		var queryWithMixedCase = new QueryDefinition("SELECT c.ID, c.NAme, c.AgE, c.EmAiL FROM c WHERE c.Id = 'test-id-2'");
		var iteratorMixedCase = container.GetItemQueryIterator<TestItem>(queryWithMixedCase);
		var resultMixedCase = await iteratorMixedCase.ReadNextAsync();

		_output.WriteLine("TEST WITH MIXED CASE PROPERTIES:");
		_output.WriteLine($"Result count: {resultMixedCase.Count}");
		if (resultMixedCase.Any())
		{
			var item = resultMixedCase.First();
			_output.WriteLine($"Item values: Id={item.Id}, Name={item.Name}, Age={item.Age}, Email={item.Email}");
		}

		// Assert - If case insensitive, this should pass
		Assert.Single(resultMixedCase);
		if (resultMixedCase.Any())
		{
			var itemMixedCase = resultMixedCase.First();
			Assert.Equal(originalItem.Id, itemMixedCase.Id);
			Assert.Equal(originalItem.Name, itemMixedCase.Name);
			Assert.Equal(originalItem.Age, itemMixedCase.Age);
			Assert.Equal(originalItem.Email, itemMixedCase.Email);
		}
	}
}
