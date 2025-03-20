using Microsoft.Azure.Cosmos;

namespace TimAbell.FakeCosmosDb.Implementation;

public class FakeContainerResponse(Container container) : ContainerResponse
{
	public override Container Container => container;
}
