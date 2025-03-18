using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using TimAbell.FakeCosmosDb.Tests.Utilities;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests;

public class CosmosDbTests
{
	private static ILogger _logger;

	public CosmosDbTests(ITestOutputHelper output)
	{
		_logger = new TestLogger(output);
	}

	[Theory]
	[MemberData(nameof(TestConfigurations))]
	public async Task Can_Insert_And_Query_Item(ICosmosDb db)
	{
		var containerName = "TestContainer";
		await db.AddContainerAsync(containerName);

		var user = new { id = "1", Name = "Alice", Age = 30 };
		await db.AddItemAsync(containerName, user);

		var results = await db.QueryAsync(containerName, "SELECT * FROM c WHERE c.Name = 'Alice'");

		Assert.Single(results);
		Assert.Equal("Alice", results.First()["Name"]);
	}

	public static IEnumerable<object[]> TestConfigurations()
	{
		yield return new object[] { new CosmosInMemoryCosmosDb(_logger) };
		// Skip the real adapter in normal test runs
		// yield return new object[] { new CosmosDbAdapter("AccountEndpoint=https://localhost:8081;AccountKey=your-key;") };
	}
}
