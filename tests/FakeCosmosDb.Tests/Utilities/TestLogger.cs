using System;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests.Utilities
{
	/// <summary>
	/// A test logger that outputs to xUnit's ITestOutputHelper
	/// </summary>
	public class TestLogger : ILogger
	{
		private readonly ITestOutputHelper _output;

		public TestLogger(ITestOutputHelper output)
		{
			_output = output;
		}

		public IDisposable BeginScope<TState>(TState state)
		{
			return null;
		}

		public bool IsEnabled(LogLevel logLevel)
		{
			return true;
		}

		public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
		{
			_output.WriteLine($"{logLevel}: {formatter(state, exception)}");
			if (exception != null)
			{
				_output.WriteLine($"Exception: {exception}");
			}
		}
	}
}
