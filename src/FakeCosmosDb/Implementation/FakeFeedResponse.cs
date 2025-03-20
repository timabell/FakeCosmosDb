using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Microsoft.Azure.Cosmos;

namespace TimAbell.FakeCosmosDb.Implementation;

public class FakeFeedResponse<T>(IEnumerable<T> items, string continuationToken) : FeedResponse<T>, IEnumerable
{
	// Base Response<T> implementation
	public override HttpStatusCode StatusCode => HttpStatusCode.OK;
	public override IEnumerable<T> Resource => items;
	public override CosmosDiagnostics Diagnostics => null;
	
	// FeedResponse<T> implementation
	public override string IndexMetrics => string.Empty;
	public override Headers Headers => new();
	public override string ContinuationToken => continuationToken;
	public override double RequestCharge => 0;
	public override string ActivityId => Guid.NewGuid().ToString();
	public override string ETag => string.Empty;
	public override IEnumerator<T> GetEnumerator() => items.GetEnumerator();
	public override int Count => items.Count();
	
	IEnumerator IEnumerable.GetEnumerator() => items.GetEnumerator();
}
