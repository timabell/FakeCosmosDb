using System;
using System.Linq;
using System.Threading.Tasks;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using TimAbell.FakeCosmosDb.Tests.Utilities;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests.SqlQueryTests
{
	public class CosmosDbDataTypeFilterTests
	{
		private readonly FakeCosmosDb _db;
		private readonly string _containerName = "DataTypeTests";
		private readonly ITestOutputHelper _output;
		private readonly ILogger _logger;
		private readonly string _databaseName = "TestDatabase";

		public CosmosDbDataTypeFilterTests(ITestOutputHelper output)
		{
			_output = output;
			_logger = new TestLogger(output);
			_db = new FakeCosmosDb(_logger);
			// Container is created by GetContainer, no need for AddContainerAsync
		}

		private async Task InitializeAsync()
		{
			await _db.CreateDatabaseIfNotExistsAsync(_databaseName);
			// Container is created by GetContainer, no need for additional calls
		}

		private async Task AddTestItemAsync<T>(T item, string containerName)
		{
			var container = _db.GetContainer(_databaseName, containerName);
			await container.CreateItemAsync(item);
		}

		[Fact]
		public async Task Query_WithIntegerFilters_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_Integers";
			var container = _db.GetContainer(_databaseName, containerName);
			await SeedTestDataWithIntegers(containerName);

			// Act & Assert - Equal
			var queryDefinitionEqual = new QueryDefinition("SELECT * FROM c WHERE c.Age = 30");
			var iteratorEqual = container.GetItemQueryIterator<JObject>(queryDefinitionEqual);
			var resultsEqual = await iteratorEqual.ReadNextAsync();

			resultsEqual.Should().HaveCount(1, "because only one document has Age = 30");
			resultsEqual.First()["Name"].Value<string>().Should().Be("John");

			// Act & Assert - Greater Than
			var queryDefinitionGreater = new QueryDefinition("SELECT * FROM c WHERE c.Age > 25");
			var iteratorGreater = container.GetItemQueryIterator<JObject>(queryDefinitionGreater);
			var resultsGreater = await iteratorGreater.ReadNextAsync();

			resultsGreater.Should().HaveCount(2, "because two documents have Age > 25");
			resultsGreater.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "John", "Alice" });

			// Act & Assert - Less Than
			var queryDefinitionLess = new QueryDefinition("SELECT * FROM c WHERE c.Age < 25");
			var iteratorLess = container.GetItemQueryIterator<JObject>(queryDefinitionLess);
			var resultsLess = await iteratorLess.ReadNextAsync();

			resultsLess.Should().HaveCount(1, "because one document has Age < 25");
			resultsLess.First()["Name"].Value<string>().Should().Be("Bob");

			// Act & Assert - Between
			var queryDefinitionBetween = new QueryDefinition("SELECT * FROM c WHERE c.Age BETWEEN 20 AND 27");
			var iteratorBetween = container.GetItemQueryIterator<JObject>(queryDefinitionBetween);
			var resultsBetween = await iteratorBetween.ReadNextAsync();

			resultsBetween.Should().HaveCount(2, "because two documents have 20 <= Age <= 27");
			resultsBetween.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Jane", "Bob" });
		}

		[Fact]
		public async Task Query_WithFloatFilters_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_Floats";
			var container = _db.GetContainer(_databaseName, containerName);
			await SeedTestDataWithFloats(containerName);

			// Act & Assert - Equal
			var queryDefinitionEqual = new QueryDefinition("SELECT * FROM c WHERE c.Score = 85.5");
			var iteratorEqual = container.GetItemQueryIterator<JObject>(queryDefinitionEqual);
			var resultsEqual = await iteratorEqual.ReadNextAsync();

			resultsEqual.Should().HaveCount(1, "because only one document has Score = 85.5");
			resultsEqual.First()["Name"].Value<string>().Should().Be("John");

			// Act & Assert - Greater Than
			var queryDefinitionGreater = new QueryDefinition("SELECT * FROM c WHERE c.Score > 85.5");
			var iteratorGreater = container.GetItemQueryIterator<JObject>(queryDefinitionGreater);
			var resultsGreater = await iteratorGreater.ReadNextAsync();

			resultsGreater.Should().HaveCount(1, "because one document has Score > 85.5");
			resultsGreater.First()["Name"].Value<string>().Should().Be("Bob");

			// Act & Assert - Less Than
			var queryDefinitionLess = new QueryDefinition("SELECT * FROM c WHERE c.Score < 85.5");
			var iteratorLess = container.GetItemQueryIterator<JObject>(queryDefinitionLess);
			var resultsLess = await iteratorLess.ReadNextAsync();

			resultsLess.Should().HaveCount(2, "because two documents have Score < 85.5");
			resultsLess.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Jane", "Alice" });
		}

		[Fact]
		public async Task Query_WithBooleanFilters_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_Booleans";
			var container = _db.GetContainer(_databaseName, containerName);
			await SeedTestDataWithBooleans(containerName);

			// Act & Assert - True
			var queryDefinitionTrue = new QueryDefinition("SELECT * FROM c WHERE c.IsActive = true");
			var iteratorTrue = container.GetItemQueryIterator<JObject>(queryDefinitionTrue);
			var resultsTrue = await iteratorTrue.ReadNextAsync();

			resultsTrue.Should().HaveCount(2, "because two documents have IsActive = true");
			resultsTrue.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "John", "Jane" });

			// Act & Assert - False
			var queryDefinitionFalse = new QueryDefinition("SELECT * FROM c WHERE c.IsActive = false");
			var iteratorFalse = container.GetItemQueryIterator<JObject>(queryDefinitionFalse);
			var resultsFalse = await iteratorFalse.ReadNextAsync();

			resultsFalse.Should().HaveCount(2, "because two documents have IsActive = false");
			resultsFalse.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Bob", "Alice" });
		}

		[Fact]
		public async Task Query_WithDateTimeFilters_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_Dates";
			var container = _db.GetContainer(_databaseName, containerName);
			await SeedTestDataWithDates(containerName);

			// Act & Assert - Equal
			var queryDefinitionEqual = new QueryDefinition("SELECT * FROM c WHERE c.CreatedDate = '2023-01-01T12:00:00Z'");
			var iteratorEqual = container.GetItemQueryIterator<JObject>(queryDefinitionEqual);
			var resultsEqual = await iteratorEqual.ReadNextAsync();

			resultsEqual.Should().HaveCount(1, "because one document has CreatedDate = 2023-01-01T12:00:00Z");
			resultsEqual.First()["Name"].Value<string>().Should().Be("John");

			// Act & Assert - Greater Than
			var queryDefinitionGreater = new QueryDefinition("SELECT * FROM c WHERE c.CreatedDate > '2023-01-02T00:00:00Z'");
			var iteratorGreater = container.GetItemQueryIterator<JObject>(queryDefinitionGreater);
			var resultsGreater = await iteratorGreater.ReadNextAsync();

			resultsGreater.Should().HaveCount(2, "because two documents have CreatedDate > 2023-01-02");
			resultsGreater.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Bob", "Alice" });
		}

		[Fact]
		public async Task Query_WithArrayContains_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_Arrays";
			var container = _db.GetContainer(_databaseName, containerName);
			await SeedTestDataWithArrays(containerName);

			// Act & Assert - ARRAY_CONTAINS
			var queryDefinitionPizza = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS(c.FavoriteFoods, 'Pizza')");
			var iteratorPizza = container.GetItemQueryIterator<JObject>(queryDefinitionPizza);
			var resultsPizza = await iteratorPizza.ReadNextAsync();

			resultsPizza.Should().HaveCount(2, "because two documents have Pizza in FavoriteFoods");
			resultsPizza.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "John", "Bob" });

			// Act & Assert - Multiple array contains
			var queryDefinitionSushi = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS(c.FavoriteFoods, 'Sushi')");
			var iteratorSushi = container.GetItemQueryIterator<JObject>(queryDefinitionSushi);
			var resultsSushi = await iteratorSushi.ReadNextAsync();

			resultsSushi.Should().HaveCount(2, "because two documents have Sushi in FavoriteFoods");
			resultsSushi.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Jane", "Alice" });
		}

		[Fact]
		public async Task Query_WithNestedObjectFilters_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_NestedObjects";
			var container = _db.GetContainer(_databaseName, containerName);
			await SeedTestDataWithNestedObjects(containerName);

			// Act & Assert - Nested property equals
			var queryDefinitionNested = new QueryDefinition("SELECT * FROM c WHERE c.Address.City = 'New York'");
			var iteratorNested = container.GetItemQueryIterator<JObject>(queryDefinitionNested);
			var resultsNested = await iteratorNested.ReadNextAsync();

			resultsNested.Should().HaveCount(1, "because only one document has Address.City = New York");
			resultsNested.First()["Name"].Value<string>().Should().Be("John");

			// Act & Assert - Multiple nested properties
			var queryDefinitionDeepNested = new QueryDefinition("SELECT * FROM c WHERE c.Address.Location.Zip = '10001'");
			var iteratorDeepNested = container.GetItemQueryIterator<JObject>(queryDefinitionDeepNested);
			var resultsDeepNested = await iteratorDeepNested.ReadNextAsync();

			resultsDeepNested.Should().HaveCount(1, "because only one document has Address.Location.Zip = 10001");
			resultsDeepNested.First()["Name"].Value<string>().Should().Be("John");
		}

		[Fact]
		public async Task Query_WithNullFilters_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_NullValues";
			var container = _db.GetContainer(_databaseName, containerName);
			await SeedTestDataWithNullValues(containerName);

			// Act & Assert - IS NULL
			var queryDefinitionIsNull = new QueryDefinition("SELECT * FROM c WHERE IS_NULL(c.OptionalField)");
			var iteratorIsNull = container.GetItemQueryIterator<JObject>(queryDefinitionIsNull);
			var resultsIsNull = await iteratorIsNull.ReadNextAsync();

			resultsIsNull.Should().HaveCount(2, "because two documents have OptionalField = null");
			resultsIsNull.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Bob", "Alice" });

			// Act & Assert - IS NOT NULL
			var queryDefinitionIsNotNull = new QueryDefinition("SELECT * FROM c WHERE NOT IS_NULL(c.OptionalField)");
			var iteratorIsNotNull = container.GetItemQueryIterator<JObject>(queryDefinitionIsNotNull);
			var resultsIsNotNull = await iteratorIsNotNull.ReadNextAsync();

			resultsIsNotNull.Should().HaveCount(2, "because two documents have OptionalField != null");
			resultsIsNotNull.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "John", "Jane" });
		}

		[Fact]
		public async Task Query_WithIsDefinedFunction_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_DefinedFields";
			await SeedTestDataWithDefinedAndUndefinedFields(containerName);
			var container = _db.GetContainer(_databaseName, containerName);

			// Act & Assert - IS_DEFINED for field that exists in all documents (some null, some with values)
			var queryDefinitionIsDefined = new QueryDefinition("SELECT * FROM c WHERE IS_DEFINED(c.OptionalField)");
			var iteratorIsDefined = container.GetItemQueryIterator<JObject>(queryDefinitionIsDefined);
			var resultsIsDefined = await iteratorIsDefined.ReadNextAsync();

			resultsIsDefined.Should().HaveCount(4, "because all four documents have OptionalField defined (even if null)");

			// Act & Assert - IS_DEFINED for field that only exists in some documents
			var queryDefinitionIsDefinedPartial = new QueryDefinition("SELECT * FROM c WHERE IS_DEFINED(c.ConditionalField)");
			var iteratorIsDefinedPartial = container.GetItemQueryIterator<JObject>(queryDefinitionIsDefinedPartial);
			var resultsIsDefinedPartial = await iteratorIsDefinedPartial.ReadNextAsync();

			resultsIsDefinedPartial.Should().HaveCount(2, "because only two documents have ConditionalField defined");
			resultsIsDefinedPartial.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "John", "Jane" });

			// Act & Assert - NOT IS_DEFINED for field that only exists in some documents
			var queryDefinitionNotDefined = new QueryDefinition("SELECT * FROM c WHERE NOT IS_DEFINED(c.ConditionalField)");
			var iteratorNotDefined = container.GetItemQueryIterator<JObject>(queryDefinitionNotDefined);
			var resultsNotDefined = await iteratorNotDefined.ReadNextAsync();

			resultsNotDefined.Should().HaveCount(2, "because two documents don't have ConditionalField defined");
			resultsNotDefined.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Bob", "Alice" });
		}

		[Fact]
		public async Task Query_WithNotOperator_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_NotOperator";
			await SeedTestDataForLogicalOperators(containerName);
			var container = _db.GetContainer(_databaseName, containerName);

			// Act & Assert - NOT with boolean property
			var queryDefinitionNotPremium = new QueryDefinition("SELECT * FROM c WHERE NOT c.IsPremium");
			var iteratorNotPremium = container.GetItemQueryIterator<JObject>(queryDefinitionNotPremium);
			var resultsNotPremium = await iteratorNotPremium.ReadNextAsync();

			resultsNotPremium.Should().HaveCount(2, "because two documents have IsPremium = false");
			resultsNotPremium.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Bob", "Alice" });

			// Act & Assert - NOT with comparison
			var queryDefinitionNotEngineering = new QueryDefinition("SELECT * FROM c WHERE NOT (c.Department = 'Engineering')");
			var iteratorNotEngineering = container.GetItemQueryIterator<JObject>(queryDefinitionNotEngineering);
			var resultsNotEngineering = await iteratorNotEngineering.ReadNextAsync();

			resultsNotEngineering.Should().HaveCount(2, "because two documents have Department != Engineering");
			resultsNotEngineering.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Jane", "Alice" });

			// Act & Assert - NOT with function
			var queryDefinitionNotArray = new QueryDefinition("SELECT * FROM c WHERE NOT ARRAY_CONTAINS(c.Tags, 'senior')");
			var iteratorNotArray = container.GetItemQueryIterator<JObject>(queryDefinitionNotArray);
			var resultsNotArray = await iteratorNotArray.ReadNextAsync();

			resultsNotArray.Should().HaveCount(1, "because one document doesn't have 'senior' in Tags");
			resultsNotArray.First()["Name"].Value<string>().Should().Be("Jane");
		}

		[Fact]
		public async Task Query_WithOrOperator_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_OrOperator";
			var container = _db.GetContainer(_databaseName, containerName);
			await SeedTestDataForLogicalOperators(containerName);

			// Act & Assert - Simple OR between two conditions
			var queryDefinitionSalesOrMarketing = new QueryDefinition("SELECT * FROM c WHERE c.Department = 'Sales' OR c.Department = 'Marketing'");
			var iteratorSalesOrMarketing = container.GetItemQueryIterator<JObject>(queryDefinitionSalesOrMarketing);
			var resultsSalesOrMarketing = await iteratorSalesOrMarketing.ReadNextAsync();

			resultsSalesOrMarketing.Should().HaveCount(2, "because two documents are in Sales or Marketing");
			resultsSalesOrMarketing.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Jane", "Alice" });

			// Act & Assert - OR with numeric comparison
			var queryDefinitionYoungOrOld = new QueryDefinition("SELECT * FROM c WHERE c.Age < 30 OR c.Age >= 40");
			var iteratorYoungOrOld = container.GetItemQueryIterator<JObject>(queryDefinitionYoungOrOld);
			var resultsYoungOrOld = await iteratorYoungOrOld.ReadNextAsync();

			resultsYoungOrOld.Should().HaveCount(2, "because three documents have Age < 30 or Age >= 40");
			resultsYoungOrOld.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Jane", "Alice" });

			// Act & Assert - OR with multiple criteria types
			var queryDefinitionPremiumOrOld = new QueryDefinition("SELECT * FROM c WHERE c.IsPremium OR c.Age >= 35");
			var iteratorPremiumOrOld = container.GetItemQueryIterator<JObject>(queryDefinitionPremiumOrOld);
			var resultsPremiumOrOld = await iteratorPremiumOrOld.ReadNextAsync();

			resultsPremiumOrOld.Should().HaveCount(3, "because all documents match one of these criteria");
			resultsPremiumOrOld.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Jane", "Alice", "John" });
		}

		[Fact]
		public async Task Query_WithComplexLogicalExpressions_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_ComplexLogical";
			var container = _db.GetContainer(_databaseName, containerName);
			await SeedTestDataForLogicalOperators(containerName);

			// Act & Assert - AND with OR
			var queryDefinitionEngineeringAndSeniorOrPremium = new QueryDefinition("SELECT * FROM c WHERE c.Department = 'Engineering' AND (ARRAY_CONTAINS(c.Tags, 'senior') OR c.IsPremium)");
			var iteratorEngineeringAndSeniorOrPremium = container.GetItemQueryIterator<JObject>(queryDefinitionEngineeringAndSeniorOrPremium);
			var resultsEngineeringAndSeniorOrPremium = await iteratorEngineeringAndSeniorOrPremium.ReadNextAsync();

			resultsEngineeringAndSeniorOrPremium.Should().HaveCount(2, "because two Engineering documents are either senior or premium");
			resultsEngineeringAndSeniorOrPremium.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "John", "Bob" });

			// Act & Assert - NOT with AND
			var queryDefinitionNotPremiumAndEngineering = new QueryDefinition("SELECT * FROM c WHERE NOT c.IsPremium AND c.Department = 'Engineering'");
			var iteratorNotPremiumAndEngineering = container.GetItemQueryIterator<JObject>(queryDefinitionNotPremiumAndEngineering);
			var resultsNotPremiumAndEngineering = await iteratorNotPremiumAndEngineering.ReadNextAsync();

			resultsNotPremiumAndEngineering.Should().HaveCount(1, "because one document is Engineering but not Premium");
			resultsNotPremiumAndEngineering.First()["Name"].Value<string>().Should().Be("Bob");

			// Act & Assert - Complex expression with multiple ANDs and ORs
			var queryDefinitionComplex = new QueryDefinition("SELECT * FROM c WHERE (c.Age > 30 AND c.Department = 'Engineering') OR (c.Age < 30 AND c.IsPremium)");
			var iteratorComplex = container.GetItemQueryIterator<JObject>(queryDefinitionComplex);
			var resultsComplex = await iteratorComplex.ReadNextAsync();

			resultsComplex.Should().HaveCount(2, "because two documents match this complex condition");
			resultsComplex.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Jane", "John" });

			// Act & Assert - Complex with NOT and parentheses
			var queryDefinitionWithNotAndParentheses = new QueryDefinition("SELECT * FROM c WHERE NOT (c.Department = 'Engineering' OR c.Age <= 30)");
			var iteratorWithNotAndParentheses = container.GetItemQueryIterator<JObject>(queryDefinitionWithNotAndParentheses);
			var resultsWithNotAndParentheses = await iteratorWithNotAndParentheses.ReadNextAsync();

			resultsWithNotAndParentheses.Should().HaveCount(1, "because only one document is neither Engineering nor Age <= 30");
			resultsWithNotAndParentheses.First()["Name"].Value<string>().Should().Be("Alice");
		}

		[Fact]
		public async Task Query_WithStringFilters_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_Strings";
			var container = _db.GetContainer(_databaseName, containerName);
			await SeedTestDataWithStrings(containerName);

			// Act & Assert - Equal
			var queryDefinitionEqual = new QueryDefinition("SELECT * FROM c WHERE c.Name = 'John'");
			var iteratorEqual = container.GetItemQueryIterator<JObject>(queryDefinitionEqual);
			var resultsEqual = await iteratorEqual.ReadNextAsync();

			resultsEqual.Should().HaveCount(1, "because only one document has Name = 'John'");
			resultsEqual.First()["id"].Value<string>().Should().Be("1");

			// Act & Assert - StartsWith
			var queryDefinitionStartsWith = new QueryDefinition("SELECT * FROM c WHERE STARTSWITH(c.Name, 'J')");
			var iteratorStartsWith = container.GetItemQueryIterator<JObject>(queryDefinitionStartsWith);
			var resultsStartsWith = await iteratorStartsWith.ReadNextAsync();

			resultsStartsWith.Should().HaveCount(2, "because two documents have names starting with 'J'");
			resultsStartsWith.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "John", "Jane" });

			// Act & Assert - Contains - case-sensitive
			var queryDefinitionContains = new QueryDefinition("SELECT * FROM c WHERE CONTAINS(c.Name, 'a', false)");
			var iteratorContains = container.GetItemQueryIterator<JObject>(queryDefinitionContains);
			var resultsContains = await iteratorContains.ReadNextAsync();

			resultsContains.Should().HaveCount(1, "because one document has name containing lowercase 'a'");
			resultsContains.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Jane" });

			// Act & Assert - Contains with case-insensitive search (third parameter = true)
			var queryDefinitionContainsCaseInsensitive = new QueryDefinition("SELECT * FROM c WHERE CONTAINS(c.Name, 'A', true)");
			var iteratorContainsCaseInsensitive = container.GetItemQueryIterator<JObject>(queryDefinitionContainsCaseInsensitive);
			var resultsContainsCaseInsensitive = await iteratorContainsCaseInsensitive.ReadNextAsync();

			resultsContainsCaseInsensitive.Should().HaveCount(2, "because two documents have names containing 'A' (case-insensitive)");
			resultsContainsCaseInsensitive.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Jane", "Alice" });

			// Act & Assert - Defaults to Case Insensitive
			var queryDefinitionCaseInsensitive = new QueryDefinition("SELECT * FROM c WHERE c.Name = 'alice'");
			var iteratorCaseInsensitive = container.GetItemQueryIterator<JObject>(queryDefinitionCaseInsensitive);
			var resultsCaseInsensitive = await iteratorCaseInsensitive.ReadNextAsync();

			resultsCaseInsensitive.Should().HaveCount(0, "because SQL queries are case-sensitive by default");
		}

		#region Test Data Creation

		private async Task SeedTestDataWithIntegers(string containerName)
		{
			var container = _db.GetContainer(_databaseName, containerName);

			await container.CreateItemAsync(new
			{
				id = "1",
				Name = "John",
				Age = 30
			});

			await container.CreateItemAsync(new
			{
				id = "2",
				Name = "Jane",
				Age = 25
			});

			await container.CreateItemAsync(new
			{
				id = "3",
				Name = "Bob",
				Age = 20
			});

			await container.CreateItemAsync(new
			{
				id = "4",
				Name = "Alice",
				Age = 35
			});
		}

		private async Task SeedTestDataWithFloats(string containerName)
		{
			var container = _db.GetContainer(_databaseName, containerName);

			await container.CreateItemAsync(new
			{
				id = "1",
				Name = "John",
				Score = 85.5
			});

			await container.CreateItemAsync(new
			{
				id = "2",
				Name = "Jane",
				Score = 75.0
			});

			await container.CreateItemAsync(new
			{
				id = "3",
				Name = "Bob",
				Score = 90.3
			});

			await container.CreateItemAsync(new
			{
				id = "4",
				Name = "Alice",
				Score = 82.1
			});
		}

		private async Task SeedTestDataWithBooleans(string containerName)
		{
			var container = _db.GetContainer(_databaseName, containerName);

			await container.CreateItemAsync(new
			{
				id = "1",
				Name = "John",
				IsActive = true
			});

			await container.CreateItemAsync(new
			{
				id = "2",
				Name = "Jane",
				IsActive = true
			});

			await container.CreateItemAsync(new
			{
				id = "3",
				Name = "Bob",
				IsActive = false
			});

			await container.CreateItemAsync(new
			{
				id = "4",
				Name = "Alice",
				IsActive = false
			});
		}

		private async Task SeedTestDataWithDateTimes(string containerName)
		{
			await AddTestItemAsync(new
			{
				id = "1",
				Name = "John",
				CreatedDate = "2025-01-15T00:00:00.000Z"
			}, containerName);

			await AddTestItemAsync(new
			{
				id = "2",
				Name = "Jane",
				CreatedDate = "2025-01-01T00:00:00.000Z"
			}, containerName);

			await AddTestItemAsync(new
			{
				id = "3",
				Name = "Bob",
				CreatedDate = "2024-12-15T00:00:00.000Z"
			}, containerName);

			await AddTestItemAsync(new
			{
				id = "4",
				Name = "Alice",
				CreatedDate = "2024-12-01T00:00:00.000Z"
			}, containerName);
		}

		private async Task SeedTestDataWithDates(string containerName)
		{
			var container = _db.GetContainer(_databaseName, containerName);

			await container.CreateItemAsync(new
			{
				id = "1",
				Name = "John",
				CreatedDate = "2023-01-01T12:00:00Z"
			});

			await container.CreateItemAsync(new
			{
				id = "2",
				Name = "Jane",
				CreatedDate = "2023-01-02T00:00:00Z"
			});

			await container.CreateItemAsync(new
			{
				id = "3",
				Name = "Bob",
				CreatedDate = "2023-01-03T00:00:00Z"
			});

			await container.CreateItemAsync(new
			{
				id = "4",
				Name = "Alice",
				CreatedDate = "2023-01-04T00:00:00Z"
			});
		}

		private async Task SeedTestDataWithArrays(string containerName)
		{
			await AddTestItemAsync(new
			{
				id = "1",
				Name = "John",
				FavoriteFoods = new[] { "Pizza", "Burger", "Pasta" }
			}, containerName);

			await AddTestItemAsync(new
			{
				id = "2",
				Name = "Jane",
				FavoriteFoods = new[] { "Sushi", "Salad", "Soup" }
			}, containerName);

			await AddTestItemAsync(new
			{
				id = "3",
				Name = "Bob",
				FavoriteFoods = new[] { "Pizza", "Tacos", "Steak" }
			}, containerName);

			await AddTestItemAsync(new
			{
				id = "4",
				Name = "Alice",
				FavoriteFoods = new[] { "Sushi", "Curry", "Noodles" }
			}, containerName);
		}

		private async Task SeedTestDataWithNestedObjects(string containerName)
		{
			await AddTestItemAsync(new
			{
				id = "1",
				Name = "John",
				Address = new
				{
					Street = "123 Main St",
					City = "New York",
					State = "NY",
					Location = new
					{
						Latitude = 40.7128,
						Longitude = -74.0060,
						Zip = "10001"
					}
				}
			}, containerName);

			await AddTestItemAsync(new
			{
				id = "2",
				Name = "Jane",
				Address = new
				{
					Street = "456 Oak Ave",
					City = "Los Angeles",
					State = "CA",
					Location = new
					{
						Latitude = 34.0522,
						Longitude = -118.2437,
						Zip = "90001"
					}
				}
			}, containerName);

			await AddTestItemAsync(new
			{
				id = "3",
				Name = "Bob",
				Address = new
				{
					Street = "789 Pine St",
					City = "Chicago",
					State = "IL",
					Location = new
					{
						Latitude = 41.8781,
						Longitude = -87.6298,
						Zip = "60601"
					}
				}
			}, containerName);

			await AddTestItemAsync(new
			{
				id = "4",
				Name = "Alice",
				Address = new
				{
					Street = "101 Maple Dr",
					City = "Seattle",
					State = "WA",
					Location = new
					{
						Latitude = 47.6062,
						Longitude = -122.3321,
						Zip = "98101"
					}
				}
			}, containerName);
		}

		private async Task SeedTestDataWithNullValues(string containerName)
		{
			await AddTestItemAsync(new
			{
				id = "1",
				Name = "John",
				OptionalField = "Value1"
			}, containerName);

			await AddTestItemAsync(new
			{
				id = "2",
				Name = "Jane",
				OptionalField = "Value2"
			}, containerName);

			await AddTestItemAsync(new
			{
				id = "3",
				Name = "Bob",
				OptionalField = (string)null
			}, containerName);

			await AddTestItemAsync(new
			{
				id = "4",
				Name = "Alice",
				OptionalField = (string)null
			}, containerName);
		}

		private async Task SeedTestDataWithDefinedAndUndefinedFields(string containerName)
		{
			var container = _db.GetContainer(_databaseName, containerName);

			// Item 1: Has OptionalField with value and ConditionalField
			await container.CreateItemAsync(new
			{
				id = Guid.NewGuid().ToString(),
				Name = "John",
				OptionalField = "Has Value",
				ConditionalField = "Also Present"
			});

			// Item 2: Has OptionalField with value and ConditionalField
			await container.CreateItemAsync(new
			{
				id = Guid.NewGuid().ToString(),
				Name = "Jane",
				OptionalField = "Another Value",
				ConditionalField = "Present Here Too"
			});

			// Item 3: Has OptionalField as null but no ConditionalField
			await container.CreateItemAsync(new
			{
				id = Guid.NewGuid().ToString(),
				Name = "Bob",
				OptionalField = (string)null
			});

			// Item 4: Has OptionalField as null but no ConditionalField
			await container.CreateItemAsync(new
			{
				id = Guid.NewGuid().ToString(),
				Name = "Alice",
				OptionalField = (string)null
			});
		}

		private async Task SeedTestDataForLogicalOperators(string containerName)
		{
			var container = _db.GetContainer(_databaseName, containerName);

			// Item 1: John - Engineering, Premium, Senior, 35
			await container.CreateItemAsync(new
			{
				id = Guid.NewGuid().ToString(),
				Name = "John",
				Department = "Engineering",
				IsPremium = true,
				Age = 35,
				Tags = new[] { "senior", "developer" }
			});

			// Item 2: Jane - Sales, Premium, Junior, 25
			await container.CreateItemAsync(new
			{
				id = Guid.NewGuid().ToString(),
				Name = "Jane",
				Department = "Sales",
				IsPremium = true,
				Age = 25,
				Tags = new[] { "junior", "sales" }
			});

			// Item 3: Bob - Engineering, Not Premium, Mid-level, 30
			await container.CreateItemAsync(new
			{
				id = Guid.NewGuid().ToString(),
				Name = "Bob",
				Department = "Engineering",
				IsPremium = false,
				Age = 30,
				Tags = new[] { "mid-level", "senior", "developer" }
			});

			// Item 4: Alice - Marketing, Not Premium, Senior, 40
			await container.CreateItemAsync(new
			{
				id = Guid.NewGuid().ToString(),
				Name = "Alice",
				Department = "Marketing",
				IsPremium = false,
				Age = 40,
				Tags = new[] { "senior", "marketing" }
			});
		}

		private async Task SeedTestDataWithStrings(string containerName)
		{
			var container = _db.GetContainer(_databaseName, containerName);

			await container.CreateItemAsync(new
			{
				id = "1",
				Name = "John"
			});

			await container.CreateItemAsync(new
			{
				id = "2",
				Name = "Jane"
			});

			await container.CreateItemAsync(new
			{
				id = "3",
				Name = "Bob"
			});

			await container.CreateItemAsync(new
			{
				id = "4",
				Name = "Alice"
			});
		}

		#endregion
	}
}
