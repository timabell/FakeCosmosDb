using System;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using InMemoryCosmosDbMock.Tests.Utilities;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.MockableCosmos.Tests
{
	public class CosmosDbDataTypeFilterTests
	{
		private readonly CosmosInMemoryCosmosDb _db;
		private readonly string _containerName = "DataTypeTests";
		private readonly ITestOutputHelper _output;
		private readonly ILogger _logger;

		public CosmosDbDataTypeFilterTests(ITestOutputHelper output)
		{
			_output = output;
			_logger = new TestLogger(output);
			_db = new CosmosInMemoryCosmosDb(_logger);
			_db.AddContainerAsync(_containerName).Wait();
		}

		[Fact]
		public async Task Query_WithIntegerFilters_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_Integers";
			await _db.AddContainerAsync(containerName);
			await SeedTestDataWithIntegers(containerName);

			// Act & Assert - Equal
			var resultsEqual = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE c.Age = 30");
			resultsEqual.Should().HaveCount(1, "because only one document has Age = 30");
			resultsEqual.First()["Name"].Value<string>().Should().Be("John");

			// Act & Assert - Greater Than
			var resultsGreater = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE c.Age > 30");
			resultsGreater.Should().HaveCount(2, "because two documents have Age > 30");
			resultsGreater.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Bob", "Alice" });

			// Act & Assert - Less Than
			var resultsLess = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE c.Age < 30");
			resultsLess.Should().HaveCount(1, "because only one document has Age < 30");
			resultsLess.First()["Name"].Value<string>().Should().Be("Jane");
		}

		[Fact]
		public async Task Query_WithFloatFilters_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_Floats";
			await _db.AddContainerAsync(containerName);
			await SeedTestDataWithFloats(containerName);

			// Act & Assert - Equal
			var resultsEqual = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE c.Score = 85.5");
			resultsEqual.Should().HaveCount(1, "because only one document has Score = 85.5");
			resultsEqual.First()["Name"].Value<string>().Should().Be("John");

			// Act & Assert - Greater Than
			var resultsGreater = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE c.Score > 85.5");
			resultsGreater.Should().HaveCount(1, "because one document has Score > 85.5");
			resultsGreater.First()["Name"].Value<string>().Should().Be("Bob");

			// Act & Assert - Less Than
			var resultsLess = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE c.Score < 85.5");
			resultsLess.Should().HaveCount(2, "because two documents have Score < 85.5");
			resultsLess.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Jane", "Alice" });
		}

		[Fact]
		public async Task Query_WithBooleanFilters_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_Booleans";
			await _db.AddContainerAsync(containerName);
			await SeedTestDataWithBooleans(containerName);

			// Act & Assert - True
			var resultsTrue = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE c.IsActive = true");
			resultsTrue.Should().HaveCount(2, "because two documents have IsActive = true");
			resultsTrue.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "John", "Jane" });

			// Act & Assert - False
			var resultsFalse = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE c.IsActive = false");
			resultsFalse.Should().HaveCount(2, "because two documents have IsActive = false");
			resultsFalse.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Bob", "Alice" });
		}

		[Fact]
		public async Task Query_WithDateTimeFilters_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_DateTimes";
			await _db.AddContainerAsync(containerName);
			await SeedTestDataWithDateTimes(containerName);

			// Convert expected datetime format to match Cosmos DB query format
			string midDate = "2025-01-01T00:00:00.000Z";

			// Act & Assert - Equal
			var resultsEqual = await _db.QueryAsync(containerName, $"SELECT * FROM c WHERE c.CreatedDate = '{midDate}'");
			resultsEqual.Should().HaveCount(1, "because only one document has CreatedDate = 2025-01-01");
			resultsEqual.First()["Name"].Value<string>().Should().Be("Jane");

			// Act & Assert - Greater Than
			var resultsGreater = await _db.QueryAsync(containerName, $"SELECT * FROM c WHERE c.CreatedDate > '{midDate}'");
			resultsGreater.Should().HaveCount(1, "because one document has CreatedDate > 2025-01-01");
			resultsGreater.First()["Name"].Value<string>().Should().Be("John");

			// Act & Assert - Less Than
			var resultsLess = await _db.QueryAsync(containerName, $"SELECT * FROM c WHERE c.CreatedDate < '{midDate}'");
			resultsLess.Should().HaveCount(2, "because two documents have CreatedDate < 2025-01-01");
			resultsLess.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Bob", "Alice" });
		}

		[Fact]
		public async Task Query_WithArrayContains_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_Arrays";
			await _db.AddContainerAsync(containerName);
			await SeedTestDataWithArrays(containerName);

			// Act & Assert - ARRAY_CONTAINS
			var resultsContainsPizza = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE ARRAY_CONTAINS(c.FavoriteFoods, 'Pizza')");
			resultsContainsPizza.Should().HaveCount(2, "because two documents have Pizza in FavoriteFoods");
			resultsContainsPizza.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "John", "Bob" });

			// Act & Assert - Multiple array contains
			var resultsContainsSushi = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE ARRAY_CONTAINS(c.FavoriteFoods, 'Sushi')");
			resultsContainsSushi.Should().HaveCount(2, "because two documents have Sushi in FavoriteFoods");
			resultsContainsSushi.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Jane", "Alice" });
		}

		[Fact]
		public async Task Query_WithNestedObjectFilters_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_NestedObjects";
			await _db.AddContainerAsync(containerName);
			await SeedTestDataWithNestedObjects(containerName);

			// Act & Assert - Nested property equals
			var resultsNested = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE c.Address.City = 'New York'");
			resultsNested.Should().HaveCount(1, "because only one document has Address.City = New York");
			resultsNested.First()["Name"].Value<string>().Should().Be("John");

			// Act & Assert - Multiple nested properties
			var resultsDeepNested = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE c.Address.Location.Zip = '10001'");
			resultsDeepNested.Should().HaveCount(1, "because only one document has Address.Location.Zip = 10001");
			resultsDeepNested.First()["Name"].Value<string>().Should().Be("John");
		}

		[Fact]
		public async Task Query_WithNullFilters_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_NullValues";
			await _db.AddContainerAsync(containerName);
			await SeedTestDataWithNullValues(containerName);

			// Act & Assert - IS NULL
			var resultsIsNull = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE IS_NULL(c.OptionalField)");
			resultsIsNull.Should().HaveCount(2, "because two documents have OptionalField = null");
			resultsIsNull.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Bob", "Alice" });

			// Act & Assert - IS NOT NULL
			var resultsIsNotNull = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE NOT IS_NULL(c.OptionalField)");
			resultsIsNotNull.Should().HaveCount(2, "because two documents have OptionalField != null");
			resultsIsNotNull.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "John", "Jane" });
		}

		[Fact]
		public async Task Query_WithIsDefinedFunction_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_DefinedFields";
			await _db.AddContainerAsync(containerName);
			await SeedTestDataWithDefinedAndUndefinedFields(containerName);

			// Act & Assert - IS_DEFINED for field that exists in all documents (some null, some with values)
			var resultsIsDefined = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE IS_DEFINED(c.OptionalField)");
			resultsIsDefined.Should().HaveCount(4, "because all four documents have OptionalField defined (even if null)");

			// Act & Assert - IS_DEFINED for field that only exists in some documents
			var resultsIsDefinedPartial = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE IS_DEFINED(c.ConditionalField)");
			resultsIsDefinedPartial.Should().HaveCount(2, "because only two documents have ConditionalField defined");
			resultsIsDefinedPartial.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "John", "Jane" });

			// Act & Assert - NOT IS_DEFINED for field that only exists in some documents
			var resultsNotDefined = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE NOT IS_DEFINED(c.ConditionalField)");
			resultsNotDefined.Should().HaveCount(2, "because two documents don't have ConditionalField defined");
			resultsNotDefined.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Bob", "Alice" });
		}

		[Fact]
		public async Task Query_WithNotOperator_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_NotOperator";
			await _db.AddContainerAsync(containerName);
			await SeedTestDataForLogicalOperators(containerName);

			// Act & Assert - NOT with boolean property
			var resultsNotPremium = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE NOT c.IsPremium");
			resultsNotPremium.Should().HaveCount(2, "because two documents have IsPremium = false");
			resultsNotPremium.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Bob", "Alice" });

			// Act & Assert - NOT with comparison
			var resultsNotEngineering = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE NOT (c.Department = 'Engineering')");
			resultsNotEngineering.Should().HaveCount(2, "because two documents have Department != Engineering");
			resultsNotEngineering.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Jane", "Alice" });

			// Act & Assert - NOT with function
			var resultsNotArray = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE NOT ARRAY_CONTAINS(c.Tags, 'senior')");
			resultsNotArray.Should().HaveCount(1, "because one document doesn't have 'senior' in Tags");
			resultsNotArray.First()["Name"].Value<string>().Should().Be("Jane");
		}

		[Fact]
		public async Task Query_WithOrOperator_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_OrOperator";
			await _db.AddContainerAsync(containerName);
			await SeedTestDataForLogicalOperators(containerName);

			// Act & Assert - Simple OR between two conditions
			var resultsSalesOrMarketing = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE c.Department = 'Sales' OR c.Department = 'Marketing'");
			resultsSalesOrMarketing.Should().HaveCount(2, "because two documents are in Sales or Marketing");
			resultsSalesOrMarketing.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Jane", "Alice" });

			// Act & Assert - OR with numeric comparison
			var resultsYoungOrOld = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE c.Age < 30 OR c.Age >= 40");
			resultsYoungOrOld.Should().HaveCount(2, "because three documents have Age < 30 or Age >= 40");
			resultsYoungOrOld.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Jane", "Bob" });

			// Act & Assert - OR with multiple criteria types
			var resultsPremiumOrOld = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE c.IsPremium OR c.Age >= 35");
			resultsPremiumOrOld.Should().HaveCount(4, "because all documents match one of these criteria");
		}

		[Fact]
		public async Task Query_WithComplexLogicalExpressions_ReturnsCorrectResults()
		{
			// Arrange - Use a specific container for this test
			string containerName = $"{_containerName}_ComplexLogical";
			await _db.AddContainerAsync(containerName);
			await SeedTestDataForLogicalOperators(containerName);

			// Act & Assert - AND with OR
			var resultsEngineeringAndSeniorOrPremium = await _db.QueryAsync(containerName,
				"SELECT * FROM c WHERE c.Department = 'Engineering' AND (ARRAY_CONTAINS(c.Tags, 'senior') OR c.IsPremium)");
			resultsEngineeringAndSeniorOrPremium.Should().HaveCount(2, "because two Engineering documents are either senior or premium");
			resultsEngineeringAndSeniorOrPremium.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "John", "Bob" });

			// Act & Assert - NOT with AND
			var resultsNotPremiumAndEngineering = await _db.QueryAsync(containerName,
				"SELECT * FROM c WHERE NOT c.IsPremium AND c.Department = 'Engineering'");
			resultsNotPremiumAndEngineering.Should().HaveCount(1, "because one document is Engineering but not Premium");
			resultsNotPremiumAndEngineering.First()["Name"].Value<string>().Should().Be("Bob");

			// Act & Assert - Complex expression with multiple ANDs and ORs
			var resultsComplex = await _db.QueryAsync(containerName,
				"SELECT * FROM c WHERE (c.Age > 30 AND c.Department = 'Engineering') OR (c.Age < 30 AND c.IsPremium)");
			resultsComplex.Should().HaveCount(2, "because two documents match this complex condition");
			resultsComplex.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Jane", "Bob" });

			// Act & Assert - Complex with NOT and parentheses
			var resultsWithNotAndParentheses = await _db.QueryAsync(containerName,
				"SELECT * FROM c WHERE NOT (c.Department = 'Engineering' OR c.Age <= 30)");
			resultsWithNotAndParentheses.Should().HaveCount(1, "because only one document is neither Engineering nor Age <= 30");
			resultsWithNotAndParentheses.First()["Name"].Value<string>().Should().Be("Alice");
		}

		#region Test Data Creation

		private async Task SeedTestDataWithIntegers(string containerName)
		{
			await _db.AddItemAsync(containerName, new
			{
				id = "1",
				Name = "John",
				Age = 30
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "2",
				Name = "Jane",
				Age = 25
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "3",
				Name = "Bob",
				Age = 35
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "4",
				Name = "Alice",
				Age = 40
			});
		}

		private async Task SeedTestDataWithFloats(string containerName)
		{
			await _db.AddItemAsync(containerName, new
			{
				id = "1",
				Name = "John",
				Score = 85.5
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "2",
				Name = "Jane",
				Score = 75.0
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "3",
				Name = "Bob",
				Score = 90.3
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "4",
				Name = "Alice",
				Score = 82.1
			});
		}

		private async Task SeedTestDataWithBooleans(string containerName)
		{
			await _db.AddItemAsync(containerName, new
			{
				id = "1",
				Name = "John",
				IsActive = true
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "2",
				Name = "Jane",
				IsActive = true
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "3",
				Name = "Bob",
				IsActive = false
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "4",
				Name = "Alice",
				IsActive = false
			});
		}

		private async Task SeedTestDataWithDateTimes(string containerName)
		{
			await _db.AddItemAsync(containerName, new
			{
				id = "1",
				Name = "John",
				CreatedDate = "2025-01-15T00:00:00.000Z"
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "2",
				Name = "Jane",
				CreatedDate = "2025-01-01T00:00:00.000Z"
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "3",
				Name = "Bob",
				CreatedDate = "2024-12-15T00:00:00.000Z"
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "4",
				Name = "Alice",
				CreatedDate = "2024-12-01T00:00:00.000Z"
			});
		}

		private async Task SeedTestDataWithArrays(string containerName)
		{
			await _db.AddItemAsync(containerName, new
			{
				id = "1",
				Name = "John",
				FavoriteFoods = new[] { "Pizza", "Burger", "Pasta" }
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "2",
				Name = "Jane",
				FavoriteFoods = new[] { "Sushi", "Salad", "Soup" }
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "3",
				Name = "Bob",
				FavoriteFoods = new[] { "Pizza", "Tacos", "Steak" }
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "4",
				Name = "Alice",
				FavoriteFoods = new[] { "Sushi", "Curry", "Noodles" }
			});
		}

		private async Task SeedTestDataWithNestedObjects(string containerName)
		{
			await _db.AddItemAsync(containerName, new
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
			});

			await _db.AddItemAsync(containerName, new
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
			});

			await _db.AddItemAsync(containerName, new
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
			});

			await _db.AddItemAsync(containerName, new
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
			});
		}

		private async Task SeedTestDataWithNullValues(string containerName)
		{
			await _db.AddItemAsync(containerName, new
			{
				id = "1",
				Name = "John",
				OptionalField = "Value1"
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "2",
				Name = "Jane",
				OptionalField = "Value2"
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "3",
				Name = "Bob",
				OptionalField = (string)null
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "4",
				Name = "Alice",
				OptionalField = (string)null
			});
		}

		private async Task SeedTestDataWithDefinedAndUndefinedFields(string containerName)
		{
			await _db.AddItemAsync(containerName, new
			{
				id = "1",
				Name = "John",
				OptionalField = "Value1",
				ConditionalField = "ExistsHere1"
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "2",
				Name = "Jane",
				OptionalField = "Value2",
				ConditionalField = "ExistsHere2"
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "3",
				Name = "Bob",
				OptionalField = (string)null
				// ConditionalField intentionally omitted
			});

			await _db.AddItemAsync(containerName, new
			{
				id = "4",
				Name = "Alice",
				OptionalField = (string)null
				// ConditionalField intentionally omitted
			});
		}

		private async Task SeedTestDataForLogicalOperators(string containerName)
		{
			// Create multiple documents with varied properties for testing logical operators
			await _db.AddItemAsync(containerName, new JObject
			{
				["id"] = "doc1",
				["Name"] = "John",
				["Age"] = 30,
				["IsPremium"] = true,
				["Department"] = "Engineering",
				["Tags"] = JArray.FromObject(new[] { "senior", "backend", "dotnet" })
			});

			await _db.AddItemAsync(containerName, new JObject
			{
				["id"] = "doc2",
				["Name"] = "Jane",
				["Age"] = 25,
				["IsPremium"] = true,
				["Department"] = "Marketing",
				["Tags"] = JArray.FromObject(new[] { "junior", "design", "content" })
			});

			await _db.AddItemAsync(containerName, new JObject
			{
				["id"] = "doc3",
				["Name"] = "Bob",
				["Age"] = 40,
				["IsPremium"] = false,
				["Department"] = "Engineering",
				["Tags"] = JArray.FromObject(new[] { "senior", "frontend", "react" })
			});

			await _db.AddItemAsync(containerName, new JObject
			{
				["id"] = "doc4",
				["Name"] = "Alice",
				["Age"] = 35,
				["IsPremium"] = false,
				["Department"] = "Sales",
				["Tags"] = JArray.FromObject(new[] { "senior", "account", "client" })
			});
		}

		#endregion
	}
}
