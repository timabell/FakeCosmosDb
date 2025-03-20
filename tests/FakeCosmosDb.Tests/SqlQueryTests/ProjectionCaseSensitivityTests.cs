using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
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

	private async Task<Container> InitializeWithItemAsync(TestItem item)
	{
		await _cosmosDb.CreateDatabaseIfNotExistsAsync(_databaseName);
		var container = _cosmosDb.GetContainer(_databaseName, _containerName);
		await container.CreateItemAsync(item);
		return container;
	}

	public class TestItem
	{
		public string Id { get; set; }
		public string Name { get; set; }
		public int Age { get; set; }
		public string Email { get; set; }
	}

	[Fact]
	public async Task ExactCaseProjection_ShouldDeserializeCorrectly()
	{
		// Arrange
		var originalItem = new TestItem
		{
			Id = "test-id-1",
			Name = "John Doe",
			Age = 30,
			Email = "john.doe@example.com"
		};
		var container = await InitializeWithItemAsync(originalItem);

		// Act - Query with exactly matching property names (control test)
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

		// Assert
		Assert.Single(resultExactCase);
		var itemExactCase = resultExactCase.First();
		Assert.Equal(originalItem.Id, itemExactCase.Id);
		Assert.Equal(originalItem.Name, itemExactCase.Name);
		Assert.Equal(originalItem.Age, itemExactCase.Age);
		Assert.Equal(originalItem.Email, itemExactCase.Email);
	}

	[Fact]
	public async Task LowercaseProjection_ShouldDeserializeCorrectly()
	{
		// Arrange
		var originalItem = new TestItem
		{
			Id = "test-id-1",
			Name = "John Doe",
			Age = 30,
			Email = "john.doe@example.com"
		};
		var container = await InitializeWithItemAsync(originalItem);

		// Act - Query with lowercase property names
		var queryLowerCaseFields = new QueryDefinition("SELECT c.id, c.name, c.age, c.email FROM c WHERE c.Id = 'test-id-1'");
		var iteratorLowerCaseFields = container.GetItemQueryIterator<TestItem>(queryLowerCaseFields);
		var resultLowerCaseFields = await iteratorLowerCaseFields.ReadNextAsync();

		_output.WriteLine("TEST WITH LOWERCASE FIELDS:");
		_output.WriteLine($"Result count: {resultLowerCaseFields.Count}");
		if (resultLowerCaseFields.Any())
		{
			var item = resultLowerCaseFields.First();
			_output.WriteLine($"Item values: Id={item.Id}, Name={item.Name}, Age={item.Age}, Email={item.Email}");
		}

		// Assert
		Assert.Single(resultLowerCaseFields);
		var itemLowerCase = resultLowerCaseFields.First();
		Assert.Equal(originalItem.Id, itemLowerCase.Id);
		Assert.Equal(originalItem.Name, itemLowerCase.Name);
		Assert.Equal(originalItem.Age, itemLowerCase.Age);
		Assert.Equal(originalItem.Email, itemLowerCase.Email);
	}

	[Fact]
	public async Task LowercaseWhereAndProjection_ShouldDeserializeCorrectly()
	{
		// Arrange
		var originalItem = new TestItem
		{
			Id = "test-id-1",
			Name = "John Doe",
			Age = 30,
			Email = "john.doe@example.com"
		};
		var container = await InitializeWithItemAsync(originalItem);

		// Act - Query with lowercase property names in both WHERE and SELECT
		var queryLowerCase = new QueryDefinition("SELECT c.id, c.name, c.age, c.email FROM c WHERE c.id = 'test-id-1'");
		var iteratorLowerCase = container.GetItemQueryIterator<TestItem>(queryLowerCase);
		var resultLowerCase = await iteratorLowerCase.ReadNextAsync();

		_output.WriteLine("TEST WITH LOWERCASE WHERE & PROPERTIES:");
		_output.WriteLine($"Result count: {resultLowerCase.Count}");
		if (resultLowerCase.Any())
		{
			var item = resultLowerCase.First();
			_output.WriteLine($"Item values: Id={item.Id}, Name={item.Name}, Age={item.Age}, Email={item.Email}");
		}

		// Assert
		Assert.Single(resultLowerCase);
		var itemLowerCase = resultLowerCase.First();
		Assert.Equal(originalItem.Id, itemLowerCase.Id);
		Assert.Equal(originalItem.Name, itemLowerCase.Name);
		Assert.Equal(originalItem.Age, itemLowerCase.Age);
		Assert.Equal(originalItem.Email, itemLowerCase.Email);
	}

	[Fact]
	public async Task MixedCaseProjection_ShouldDeserializeCorrectly()
	{
		// Arrange
		var originalItem = new TestItem
		{
			Id = "test-id-2",
			Name = "Jane Smith",
			Age = 25,
			Email = "jane.smith@example.com"
		};
		var container = await InitializeWithItemAsync(originalItem);

		// Act - Use projection with mixed casing
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

		// Assert
		Assert.Single(resultMixedCase);
		var itemMixedCase = resultMixedCase.First();
		Assert.Equal(originalItem.Id, itemMixedCase.Id);
		Assert.Equal(originalItem.Name, itemMixedCase.Name);
		Assert.Equal(originalItem.Age, itemMixedCase.Age);
		Assert.Equal(originalItem.Email, itemMixedCase.Email);
	}

	[Fact]
	public async Task AllUppercaseProjection_ShouldDeserializeCorrectly()
	{
		// Arrange
		var originalItem = new TestItem
		{
			Id = "test-id-3",
			Name = "Alex Johnson",
			Age = 35,
			Email = "alex.johnson@example.com"
		};
		var container = await InitializeWithItemAsync(originalItem);

		// Act - Use projection with all uppercase
		var queryWithUppercase = new QueryDefinition("SELECT c.ID, c.NAME, c.AGE, c.EMAIL FROM c WHERE c.Id = 'test-id-3'");
		var iteratorUppercase = container.GetItemQueryIterator<TestItem>(queryWithUppercase);
		var resultUppercase = await iteratorUppercase.ReadNextAsync();

		_output.WriteLine("TEST WITH ALL UPPERCASE PROPERTIES:");
		_output.WriteLine($"Result count: {resultUppercase.Count}");
		if (resultUppercase.Any())
		{
			var item = resultUppercase.First();
			_output.WriteLine($"Item values: Id={item.Id}, Name={item.Name}, Age={item.Age}, Email={item.Email}");
		}

		// Assert
		Assert.Single(resultUppercase);
		var itemUppercase = resultUppercase.First();
		Assert.Equal(originalItem.Id, itemUppercase.Id);
		Assert.Equal(originalItem.Name, itemUppercase.Name);
		Assert.Equal(originalItem.Age, itemUppercase.Age);
		Assert.Equal(originalItem.Email, itemUppercase.Email);
	}

	[Fact]
	public async Task ExactCaseProjectionWithLowercaseWhere_ShouldDeserializeCorrectly()
	{
		// Arrange
		var originalItem = new TestItem
		{
			Id = "test-id-4",
			Name = "Sam Wilson",
			Age = 40,
			Email = "sam.wilson@example.com"
		};
		var container = await InitializeWithItemAsync(originalItem);

		// Act - Query with exact case in properties but lowercase in WHERE clause
		var query = new QueryDefinition("SELECT c.Id, c.Name, c.Age, c.Email FROM c WHERE c.id = 'test-id-4'");
		var iterator = container.GetItemQueryIterator<TestItem>(query);
		var result = await iterator.ReadNextAsync();

		_output.WriteLine("TEST WITH EXACT CASE PROPERTIES BUT LOWERCASE WHERE:");
		_output.WriteLine($"Result count: {result.Count}");
		if (result.Any())
		{
			var item = result.First();
			_output.WriteLine($"Item values: Id={item.Id}, Name={item.Name}, Age={item.Age}, Email={item.Email}");
		}

		// Assert
		Assert.Single(result);
		var itemResult = result.First();
		Assert.Equal(originalItem.Id, itemResult.Id);
		Assert.Equal(originalItem.Name, itemResult.Name);
		Assert.Equal(originalItem.Age, itemResult.Age);
		Assert.Equal(originalItem.Email, itemResult.Email);
	}
}
