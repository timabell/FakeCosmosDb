using System.Net;
using Microsoft.Azure.Cosmos;

namespace TimAbell.FakeCosmosDb.Implementation;

public class FakeItemResponse<T>(T item, HttpStatusCode statusCode, double requestCharge, string etag, Headers headers) : ItemResponse<T>
{
	public override T Resource => item;
	public override HttpStatusCode StatusCode { get; } = statusCode;
	public override double RequestCharge { get; } = requestCharge;
	public override string ETag { get; } = etag;
	public override Headers Headers { get; } = headers;
}
