using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TimAbell.MockableCosmos.Tests;

public class CosmosDbTests
{
    public static IEnumerable<object[]> TestConfigurations()
    {
        yield return new object[] { new CosmosInMemoryCosmosDb() };
        yield return new object[] { new CosmosDbAdapter("AccountEndpoint=https://localhost:8081;AccountKey=your-key;") };
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
}