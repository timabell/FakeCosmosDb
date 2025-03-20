using System;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using TimAbell.FakeCosmosDb.Tests.Utilities;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests.SqlQueryTests
{
	public class ParameterizedQueryTests
	{
		private readonly FakeCosmosDb _db;
		private readonly string _containerName = "ParameterizedTests";
		private readonly ITestOutputHelper _output;
		private readonly ILogger _logger;
		private readonly string _databaseName = "TestDatabase";

		public ParameterizedQueryTests(ITestOutputHelper output)
		{
			_output = output;
			_logger = new TestLogger(output);
			_db = new FakeCosmosDb(_logger);
		}

		private async Task InitializeAsync()
		{
			await _db.CreateDatabaseIfNotExistsAsync(_databaseName);
		}

		private async Task AddTestItemAsync<T>(T item, string containerName)
		{
			var container = _db.GetContainer(_databaseName, containerName);
			await container.CreateItemAsync(item);
		}

		private async Task<TestItem[]> SeedTestDataWithIntegers(string containerName)
		{
			var items = new[]
			{
				new TestItem { Id = "1", Name = "Item 1", Age = 25, IsActive = true },
				new TestItem { Id = "2", Name = "Item 2", Age = 30, IsActive = false },
				new TestItem { Id = "3", Name = "Item 3", Age = 35, IsActive = true },
				new TestItem { Id = "4", Name = "Item 4", Age = 40, IsActive = false },
				new TestItem { Id = "5", Name = "Item 5", Age = 30, IsActive = true }
			};

			foreach (var item in items)
			{
				await AddTestItemAsync(item, containerName);
			}

			return items;
		}

		private async Task<TestItemWithDecimal[]> SeedTestDataWithDecimals(string containerName)
		{
			var items = new[]
			{
				new TestItemWithDecimal { Id = "1", Name = "Item 1", Price = 25.99m, IsActive = true },
				new TestItemWithDecimal { Id = "2", Name = "Item 2", Price = 30.50m, IsActive = false },
				new TestItemWithDecimal { Id = "3", Name = "Item 3", Price = 35.75m, IsActive = true },
				new TestItemWithDecimal { Id = "4", Name = "Item 4", Price = 40.25m, IsActive = false },
				new TestItemWithDecimal { Id = "5", Name = "Item 5", Price = 30.50m, IsActive = true }
			};

			foreach (var item in items)
			{
				await AddTestItemAsync(item, containerName);
			}

			return items;
		}

		private async Task<TestItemWithStrings[]> SeedTestDataWithStrings(string containerName)
		{
			var items = new[]
			{
				new TestItemWithStrings { Id = "1", Name = "Alice", Email = "alice@example.com" },
				new TestItemWithStrings { Id = "2", Name = "Bob", Email = "bob@example.com" },
				new TestItemWithStrings { Id = "3", Name = "Charlie", Email = "charlie@example.com" },
				new TestItemWithStrings { Id = "4", Name = "David", Email = "david@example.com" },
				new TestItemWithStrings { Id = "5", Name = "Alice Smith", Email = "alice.smith@example.com" }
			};

			foreach (var item in items)
			{
				await AddTestItemAsync(item, containerName);
			}

			return items;
		}

		private async Task<TestItemWithDates[]> SeedTestDataWithDates(string containerName)
		{
			var items = new[]
			{
				new TestItemWithDates { Id = "1", Name = "Item 1", CreatedDate = new DateTime(2023, 1, 15) },
				new TestItemWithDates { Id = "2", Name = "Item 2", CreatedDate = new DateTime(2023, 2, 20) },
				new TestItemWithDates { Id = "3", Name = "Item 3", CreatedDate = new DateTime(2023, 3, 25) },
				new TestItemWithDates { Id = "4", Name = "Item 4", CreatedDate = new DateTime(2023, 4, 10) },
				new TestItemWithDates { Id = "5", Name = "Item 5", CreatedDate = new DateTime(2023, 2, 20) }
			};

			foreach (var item in items)
			{
				await AddTestItemAsync(item, containerName);
			}

			return items;
		}

		[Fact]
		public async Task Query_WithParameterizedIntegerFilters_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_Integers";
			var container = _db.GetContainer(_databaseName, containerName);
			var items = await SeedTestDataWithIntegers(containerName);

			// Act & Assert - Equal
			var queryDefinitionEqual = new QueryDefinition("SELECT * FROM c WHERE c.Age = @age")
				.WithParameter("@age", 30);
			var resultsEqual = await ExecuteQueryAsync<TestItem>(container, queryDefinitionEqual);
			
			_output.WriteLine($"Equal query returned {resultsEqual.Count()} items");
			resultsEqual.Count().Should().Be(2);
			resultsEqual.All(i => i.Age == 30).Should().BeTrue();

			// Act & Assert - Greater Than
			var queryDefinitionGreaterThan = new QueryDefinition("SELECT * FROM c WHERE c.Age > @minAge")
				.WithParameter("@minAge", 30);
			var resultsGreaterThan = await ExecuteQueryAsync<TestItem>(container, queryDefinitionGreaterThan);
			
			_output.WriteLine($"Greater Than query returned {resultsGreaterThan.Count()} items");
			resultsGreaterThan.Count().Should().Be(2);
			resultsGreaterThan.All(i => i.Age > 30).Should().BeTrue();

			// Act & Assert - Less Than or Equal
			var queryDefinitionLessThanOrEqual = new QueryDefinition("SELECT * FROM c WHERE c.Age <= @maxAge")
				.WithParameter("@maxAge", 30);
			var resultsLessThanOrEqual = await ExecuteQueryAsync<TestItem>(container, queryDefinitionLessThanOrEqual);
			
			_output.WriteLine($"Less Than or Equal query returned {resultsLessThanOrEqual.Count()} items");
			resultsLessThanOrEqual.Count().Should().Be(3);
			resultsLessThanOrEqual.All(i => i.Age <= 30).Should().BeTrue();
		}

		[Fact]
		public async Task Query_WithParameterizedDecimalFilters_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_Decimals";
			var container = _db.GetContainer(_databaseName, containerName);
			var items = await SeedTestDataWithDecimals(containerName);

			// Act & Assert - Equal
			var queryDefinitionEqual = new QueryDefinition("SELECT * FROM c WHERE c.Price = @price")
				.WithParameter("@price", 30.50m);
			var resultsEqual = await ExecuteQueryAsync<TestItemWithDecimal>(container, queryDefinitionEqual);
			
			_output.WriteLine($"Equal query returned {resultsEqual.Count()} items");
			resultsEqual.Count().Should().Be(2);
			resultsEqual.All(i => i.Price == 30.50m).Should().BeTrue();

			// Act & Assert - Greater Than
			var queryDefinitionGreaterThan = new QueryDefinition("SELECT * FROM c WHERE c.Price > @minPrice")
				.WithParameter("@minPrice", 30.50m);
			var resultsGreaterThan = await ExecuteQueryAsync<TestItemWithDecimal>(container, queryDefinitionGreaterThan);
			
			_output.WriteLine($"Greater Than query returned {resultsGreaterThan.Count()} items");
			resultsGreaterThan.Count().Should().Be(2);
			resultsGreaterThan.All(i => i.Price > 30.50m).Should().BeTrue();

			// Act & Assert - Between
			var queryDefinitionBetween = new QueryDefinition("SELECT * FROM c WHERE c.Price >= @minPrice AND c.Price <= @maxPrice")
				.WithParameter("@minPrice", 25.99m)
				.WithParameter("@maxPrice", 35.75m);
			var resultsBetween = await ExecuteQueryAsync<TestItemWithDecimal>(container, queryDefinitionBetween);
			
			_output.WriteLine($"Between query returned {resultsBetween.Count()} items");
			resultsBetween.Count().Should().Be(4);
			resultsBetween.All(i => i.Price >= 25.99m && i.Price <= 35.75m).Should().BeTrue();
		}

		[Fact]
		public async Task Query_WithParameterizedStringFilters_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_Strings";
			var container = _db.GetContainer(_databaseName, containerName);
			var items = await SeedTestDataWithStrings(containerName);

			// Act & Assert - Equal
			var queryDefinitionEqual = new QueryDefinition("SELECT * FROM c WHERE c.Name = @name")
				.WithParameter("@name", "Alice");
			var resultsEqual = await ExecuteQueryAsync<TestItemWithStrings>(container, queryDefinitionEqual);
			
			_output.WriteLine($"Equal query returned {resultsEqual.Count()} items");
			resultsEqual.Count().Should().Be(1);
			resultsEqual.All(i => i.Name == "Alice").Should().BeTrue();

			// Act & Assert - Contains (using CONTAINS function)
			var queryDefinitionContains = new QueryDefinition("SELECT * FROM c WHERE CONTAINS(c.Email, @domain)")
				.WithParameter("@domain", "example.com");
			var resultsContains = await ExecuteQueryAsync<TestItemWithStrings>(container, queryDefinitionContains);
			
			_output.WriteLine($"Contains query returned {resultsContains.Count()} items");
			resultsContains.Count().Should().Be(5);
			resultsContains.All(i => i.Email.Contains("example.com")).Should().BeTrue();

			// Act & Assert - StartsWith (using STARTSWITH function)
			var queryDefinitionStartsWith = new QueryDefinition("SELECT * FROM c WHERE STARTSWITH(c.Name, @prefix)")
				.WithParameter("@prefix", "A");
			var resultsStartsWith = await ExecuteQueryAsync<TestItemWithStrings>(container, queryDefinitionStartsWith);
			
			_output.WriteLine($"StartsWith query returned {resultsStartsWith.Count()} items");
			resultsStartsWith.Count().Should().Be(2);
			resultsStartsWith.All(i => i.Name.StartsWith("A")).Should().BeTrue();
		}

		[Fact]
		public async Task Query_WithParameterizedDateFilters_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_Dates";
			var container = _db.GetContainer(_databaseName, containerName);
			var items = await SeedTestDataWithDates(containerName);

			// Act & Assert - Equal
			var specificDate = new DateTime(2023, 2, 20);
			var queryDefinitionEqual = new QueryDefinition("SELECT * FROM c WHERE c.CreatedDate = @date")
				.WithParameter("@date", specificDate);
			var resultsEqual = await ExecuteQueryAsync<TestItemWithDates>(container, queryDefinitionEqual);
			
			_output.WriteLine($"Equal date query returned {resultsEqual.Count()} items");
			resultsEqual.Count().Should().Be(2);
			resultsEqual.All(i => i.CreatedDate == specificDate).Should().BeTrue();

			// Act & Assert - Greater Than
			var minDate = new DateTime(2023, 2, 20);
			var queryDefinitionGreaterThan = new QueryDefinition("SELECT * FROM c WHERE c.CreatedDate > @minDate")
				.WithParameter("@minDate", minDate);
			var resultsGreaterThan = await ExecuteQueryAsync<TestItemWithDates>(container, queryDefinitionGreaterThan);
			
			_output.WriteLine($"Greater Than date query returned {resultsGreaterThan.Count()} items");
			resultsGreaterThan.Count().Should().Be(2);
			resultsGreaterThan.All(i => i.CreatedDate > minDate).Should().BeTrue();

			// Act & Assert - Between
			var startDate = new DateTime(2023, 1, 1);
			var endDate = new DateTime(2023, 3, 1);
			var queryDefinitionBetween = new QueryDefinition("SELECT * FROM c WHERE c.CreatedDate >= @startDate AND c.CreatedDate <= @endDate")
				.WithParameter("@startDate", startDate)
				.WithParameter("@endDate", endDate);
			var resultsBetween = await ExecuteQueryAsync<TestItemWithDates>(container, queryDefinitionBetween);
			
			_output.WriteLine($"Between dates query returned {resultsBetween.Count()} items");
			resultsBetween.Count().Should().Be(3);
			resultsBetween.All(i => i.CreatedDate >= startDate && i.CreatedDate <= endDate).Should().BeTrue();
		}

		[Fact]
		public async Task Query_WithParameterizedLogicalOperators_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_LogicalOps";
			var container = _db.GetContainer(_databaseName, containerName);
			var items = await SeedTestDataWithIntegers(containerName);

			// Act & Assert - AND
			var queryDefinitionAnd = new QueryDefinition("SELECT * FROM c WHERE c.Age > @minAge AND c.IsActive = @isActive")
				.WithParameter("@minAge", 30)
				.WithParameter("@isActive", true);
			var resultsAnd = await ExecuteQueryAsync<TestItem>(container, queryDefinitionAnd);
			
			_output.WriteLine($"AND query returned {resultsAnd.Count()} items");
			resultsAnd.Count().Should().Be(1);
			resultsAnd.All(i => i.Age > 30 && i.IsActive).Should().BeTrue();

			// Act & Assert - OR
			var queryDefinitionOr = new QueryDefinition("SELECT * FROM c WHERE c.Age = @age OR c.Id = @id")
				.WithParameter("@age", 30)
				.WithParameter("@id", "1");
			var resultsOr = await ExecuteQueryAsync<TestItem>(container, queryDefinitionOr);
			
			_output.WriteLine($"OR query returned {resultsOr.Count()} items");
			resultsOr.Count().Should().Be(3);
			resultsOr.All(i => i.Age == 30 || i.Id == "1").Should().BeTrue();

			// Act & Assert - NOT
			var queryDefinitionNot = new QueryDefinition("SELECT * FROM c WHERE NOT (c.Age <= @maxAge)")
				.WithParameter("@maxAge", 30);
			var resultsNot = await ExecuteQueryAsync<TestItem>(container, queryDefinitionNot);
			
			_output.WriteLine($"NOT query returned {resultsNot.Count()} items");
			resultsNot.Count().Should().Be(2);
			resultsNot.All(i => !(i.Age <= 30)).Should().BeTrue();
		}

		private async Task<T[]> ExecuteQueryAsync<T>(Container container, QueryDefinition queryDefinition)
		{
			var iterator = container.GetItemQueryIterator<T>(queryDefinition);
			var results = await iterator.ReadNextAsync();
			return results.ToArray();
		}

		public class TestItem
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public int Age { get; set; }
			public bool IsActive { get; set; }
		}

		public class TestItemWithDecimal
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public decimal Price { get; set; }
			public bool IsActive { get; set; }
		}

		public class TestItemWithStrings
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string Email { get; set; }
		}

		public class TestItemWithDates
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public DateTime CreatedDate { get; set; }
		}
	}
}
