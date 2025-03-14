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
            var resultsIsNull = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE c.OptionalField = null");
            resultsIsNull.Should().HaveCount(2, "because two documents have OptionalField = null");
            resultsIsNull.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "Bob", "Alice" });

            // Act & Assert - IS NOT NULL
            var resultsIsNotNull = await _db.QueryAsync(containerName, "SELECT * FROM c WHERE c.OptionalField != null");
            resultsIsNotNull.Should().HaveCount(2, "because two documents have OptionalField != null");
            resultsIsNotNull.Select(r => r["Name"].Value<string>()).Should().Contain(new[] { "John", "Jane" });
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

        #endregion
    }
}
