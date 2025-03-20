using Microsoft.Azure.Cosmos;

namespace TimAbell.FakeCosmosDb.Implementation;

public class FakeDatabaseResponse(FakeDatabase database) : DatabaseResponse
{
	public override Database Database { get; } = database;
}
