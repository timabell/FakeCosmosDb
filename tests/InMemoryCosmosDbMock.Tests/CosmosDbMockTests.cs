using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TimAbell.MockableCosmos.Tests;

public class CosmosDbMockTests
{
	private readonly CosmosInMemoryCosmosDb _db = new();

	public CosmosDbMockTests()
	{
		_db.AddContainerAsync("Users").Wait();
	}

	[Fact]
	public async Task Can_Insert_And_Query_Item()
	{
		var user = new { id = "1", Name = "Alice", Age = 30 };
		await _db.AddItemAsync("Users", user);

		var results = await _db.QueryAsync("Users", "SELECT * FROM c WHERE c.Name = 'Alice'");
		Assert.Single(results);
		Assert.Equal("Alice", results.First()["Name"]);
	}
}
