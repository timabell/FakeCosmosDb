using FluentAssertions;
using Newtonsoft.Json.Linq;
using TimAbell.FakeCosmosDb.Parsing;
using TimAbell.FakeCosmosDb.Tests.Utilities;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests;

public class CosmosDbSqlQueryParserTests(ITestOutputHelper output)
{
	private readonly CosmosDbSqlQueryParser _parser = new CosmosDbSqlQueryParser(new TestLogger(output));

	[Fact]
	public void ShouldParseSimpleSelectQuery()
	{
		// Arrange
		var sql = "SELECT * FROM c";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.PropertyPaths.Should().ContainSingle("*");
		result.FromName.Should().Be("c");
	}

	[Fact]
	public void ShouldParseSelectWithProjection()
	{
		// Arrange
		var sql = "SELECT c.id, c.name, c.address.city FROM c";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.PropertyPaths.Should().HaveCount(3);
		result.PropertyPaths.Should().Contain("c.id");
		result.PropertyPaths.Should().Contain("c.name");
		result.PropertyPaths.Should().Contain("c.address.city");
		result.FromName.Should().Be("c");
	}

	[Fact]
	public void ShouldParseWhereWithSimpleCondition()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE c.age > 21";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.PropertyPaths.Should().ContainSingle("*");
		result.FromName.Should().Be("c");
		result.WhereConditions.Should().HaveCount(1);
		result.WhereConditions[0].PropertyPath.Should().Be("c.age");
		result.WhereConditions[0].Operator.Should().Be(ComparisonOperator.GreaterThan);
		result.WhereConditions[0].Value.Value<int>().Should().Be(21);
	}

	[Fact]
	public void ShouldParseWhereWithMultipleConditions()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE c.age > 21 AND c.name = 'John'";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.PropertyPaths.Should().ContainSingle("*");
		result.FromName.Should().Be("c");
		result.WhereConditions.Should().HaveCount(2);

		result.WhereConditions[0].PropertyPath.Should().Be("c.age");
		result.WhereConditions[0].Operator.Should().Be(ComparisonOperator.GreaterThan);
		result.WhereConditions[0].Value.Value<int>().Should().Be(21);

		result.WhereConditions[1].PropertyPath.Should().Be("c.name");
		result.WhereConditions[1].Operator.Should().Be(ComparisonOperator.Equals);
		result.WhereConditions[1].Value.Value<string>().Should().Be("John");
	}

	[Fact]
	public void ShouldParseOrderBy()
	{
		// Arrange
		var sql = "SELECT * FROM c ORDER BY c.age DESC, c.name ASC";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.PropertyPaths.Should().ContainSingle("*");
		result.FromName.Should().Be("c");
		result.OrderBy.Should().HaveCount(2);

		result.OrderBy[0].PropertyPath.Should().Be("c.age");
		result.OrderBy[0].Direction.Should().Be(SortDirection.Descending);

		result.OrderBy[1].PropertyPath.Should().Be("c.name");
		result.OrderBy[1].Direction.Should().Be(SortDirection.Ascending);
	}

	[Fact]
	public void ShouldParseLimit()
	{
		// Arrange
		var sql = "SELECT * FROM c LIMIT 10";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.PropertyPaths.Should().ContainSingle("*");
		result.FromName.Should().Be("c");
		result.Limit.Should().Be(10);
	}

	[Fact]
	public void ShouldParseContainsFunction()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE CONTAINS(c.name, 'John')";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.PropertyPaths.Should().ContainSingle("*");
		result.FromName.Should().Be("c");
		result.WhereConditions.Should().HaveCount(1);
		result.WhereConditions[0].PropertyPath.Should().Be("c.name");
		result.WhereConditions[0].Operator.Should().Be(ComparisonOperator.StringContains);
		result.WhereConditions[0].Value.Value<string>().Should().Be("John");
	}

	[Fact]
	public void ShouldParseStartsWithFunction()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE STARTSWITH(c.name, 'J')";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.PropertyPaths.Should().ContainSingle("*");
		result.FromName.Should().Be("c");
		result.WhereConditions.Should().HaveCount(1);
		result.WhereConditions[0].PropertyPath.Should().Be("c.name");
		result.WhereConditions[0].Operator.Should().Be(ComparisonOperator.StringStartsWith);
		result.WhereConditions[0].Value.Value<string>().Should().Be("J");
	}

	[Fact]
	public void ShouldParseCompleteQuery()
	{
		// Arrange
		var sql = "SELECT c.id, c.name, c.age FROM c WHERE c.age > 21 AND CONTAINS(c.name, 'J') ORDER BY c.age DESC LIMIT 10";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.PropertyPaths.Should().HaveCount(3);
		result.PropertyPaths.Should().Contain("c.id");
		result.PropertyPaths.Should().Contain("c.name");
		result.PropertyPaths.Should().Contain("c.age");

		result.FromName.Should().Be("c");

		result.WhereConditions.Should().HaveCount(2);
		result.WhereConditions[0].PropertyPath.Should().Be("c.age");
		result.WhereConditions[0].Operator.Should().Be(ComparisonOperator.GreaterThan);
		result.WhereConditions[0].Value.Value<int>().Should().Be(21);

		result.WhereConditions[1].PropertyPath.Should().Be("c.name");
		result.WhereConditions[1].Operator.Should().Be(ComparisonOperator.StringContains);
		result.WhereConditions[1].Value.Value<string>().Should().Be("J");

		result.OrderBy.Should().HaveCount(1);
		result.OrderBy[0].PropertyPath.Should().Be("c.age");
		result.OrderBy[0].Direction.Should().Be(SortDirection.Descending);

		result.Limit.Should().Be(10);
	}

	[Fact]
	public void ShouldHandleQuotedStringsInWhereConditions()
	{
		// Arrange
		var parser = new CosmosDbSqlQueryParser(new TestLogger(output));
		var sql = "SELECT * FROM c WHERE c.Name = 'Alice'";

		// Act
		var result = parser.Parse(sql);

		// Assert
		result.Should().NotBeNull();
		result.WhereConditions.Should().NotBeNull();
		result.WhereConditions.Should().HaveCount(1);
		result.WhereConditions[0].PropertyPath.Should().Be("c.Name");
		result.WhereConditions[0].Operator.Should().Be(ComparisonOperator.Equals);
		result.WhereConditions[0].Value.Type.Should().Be(JTokenType.String);
		result.WhereConditions[0].Value.ToString().Should().Be("Alice");
	}

	[Fact]
	public void ShouldHandleIntegrationTestQuery()
	{
		// This exact query is used in the integration test
		var parser = new CosmosDbSqlQueryParser(new TestLogger(output));
		var sql = "SELECT * FROM c WHERE c.Name = 'Alice'";

		// Act
		var result = parser.Parse(sql);

		// Output the debug info
		output.WriteLine(parser.DumpDebugInfo(sql));

		// Assert
		result.Should().NotBeNull();
		result.PropertyPaths.Should().NotBeNull();
		result.PropertyPaths.Should().HaveCount(1);
		result.PropertyPaths[0].Should().Be("*");

		result.FromName.Should().Be("c");
		result.FromAlias.Should().BeNull();

		result.WhereConditions.Should().NotBeNull();
		result.WhereConditions.Should().HaveCount(1);

		var whereCondition = result.WhereConditions[0];
		whereCondition.PropertyPath.Should().Be("c.Name");
		whereCondition.Operator.Should().Be(ComparisonOperator.Equals);
		whereCondition.Value.Type.Should().Be(JTokenType.String);
		whereCondition.Value.Value<string>().Should().Be("Alice");
	}

	[Fact]
	public void ShouldParseIsNullFunction()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE IS_NULL(c.optionalProperty)";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.Should().NotBeNull();
		result.WhereConditions.Should().NotBeNull();
		result.WhereConditions.Should().HaveCount(1);
		result.WhereConditions[0].PropertyPath.Should().Be("c.optionalProperty");
		result.WhereConditions[0].Operator.Should().Be(ComparisonOperator.Equals);
		result.WhereConditions[0].Value.Type.Should().Be(JTokenType.Null);
	}

	[Fact]
	public void ShouldParseIsDefinedFunction()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE IS_DEFINED(c.optionalProperty)";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.Should().NotBeNull();
		result.WhereConditions.Should().NotBeNull();
		result.WhereConditions.Should().HaveCount(1);
		result.WhereConditions[0].PropertyPath.Should().Be("c.optionalProperty");
		result.WhereConditions[0].Operator.Should().Be(ComparisonOperator.IsDefined);
		result.WhereConditions[0].Value.Type.Should().Be(JTokenType.Null);
	}

	[Fact]
	public void ShouldParseArrayContainsFunction()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE ARRAY_CONTAINS(c.tags, 'important')";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.Should().NotBeNull();
		result.WhereConditions.Should().NotBeNull();
		result.WhereConditions.Should().HaveCount(1);
		result.WhereConditions[0].PropertyPath.Should().Be("c.tags");
		result.WhereConditions[0].Operator.Should().Be(ComparisonOperator.ArrayContains);
		result.WhereConditions[0].Value.Type.Should().Be(JTokenType.String);
		result.WhereConditions[0].Value.Value<string>().Should().Be("important");
	}

	[Fact]
	public void ShouldParseMultipleFunctions()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE IS_DEFINED(c.name) AND ARRAY_CONTAINS(c.tags, 'important') AND IS_NULL(c.deletedAt)";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.Should().NotBeNull();
		result.WhereConditions.Should().NotBeNull();
		result.WhereConditions.Should().HaveCount(3);

		result.WhereConditions[0].PropertyPath.Should().Be("c.name");
		result.WhereConditions[0].Operator.Should().Be(ComparisonOperator.IsDefined);

		result.WhereConditions[1].PropertyPath.Should().Be("c.tags");
		result.WhereConditions[1].Operator.Should().Be(ComparisonOperator.ArrayContains);
		result.WhereConditions[1].Value.Value<string>().Should().Be("important");

		result.WhereConditions[2].PropertyPath.Should().Be("c.deletedAt");
		result.WhereConditions[2].Operator.Should().Be(ComparisonOperator.Equals);
		result.WhereConditions[2].Value.Type.Should().Be(JTokenType.Null);
	}

	[Fact]
	public void ShouldParseNotOperator()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE NOT (c.name = 'Alice')";

		// Act
		var result = _parser.Parse(sql);
		output.WriteLine(_parser.DumpDebugInfo(sql));

		// Assert
		result.Should().NotBeNull();
		result.WhereExpression.Should().NotBeNull();
		result.WhereExpression.Should().BeOfType<UnaryExpression>();
		var unaryExpr = (UnaryExpression)result.WhereExpression;
		unaryExpr.Operator.Should().Be(UnaryOperator.Not);

		// The inner expression should be a binary expression comparing name to 'Alice'
		unaryExpr.Operand.Should().BeOfType<BinaryExpression>();
		var binaryExpr = (BinaryExpression)unaryExpr.Operand;
		binaryExpr.Operator.Should().Be(BinaryOperator.Equal);
		binaryExpr.Left.Should().BeOfType<PropertyExpression>();
		((PropertyExpression)binaryExpr.Left).PropertyPath.Should().Be("c.name");
		binaryExpr.Right.Should().BeOfType<ConstantExpression>();
		((ConstantExpression)binaryExpr.Right).Value.Should().Be("Alice");
	}

	[Fact]
	public void ShouldParseOrOperator()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE c.name = 'Alice' OR c.name = 'Bob'";

		// Act
		var result = _parser.Parse(sql);
		output.WriteLine(_parser.DumpDebugInfo(sql));

		// Assert
		result.Should().NotBeNull();
		result.WhereExpression.Should().NotBeNull();
		result.WhereExpression.Should().BeOfType<BinaryExpression>();

		var orExpr = (BinaryExpression)result.WhereExpression;
		orExpr.Operator.Should().Be(BinaryOperator.Or);

		// Left side should be c.name = 'Alice'
		orExpr.Left.Should().BeOfType<BinaryExpression>();
		var leftExpr = (BinaryExpression)orExpr.Left;
		leftExpr.Operator.Should().Be(BinaryOperator.Equal);
		leftExpr.Left.Should().BeOfType<PropertyExpression>();
		((PropertyExpression)leftExpr.Left).PropertyPath.Should().Be("c.name");
		leftExpr.Right.Should().BeOfType<ConstantExpression>();
		((ConstantExpression)leftExpr.Right).Value.Should().Be("Alice");

		// Right side should be c.name = 'Bob'
		orExpr.Right.Should().BeOfType<BinaryExpression>();
		var rightExpr = (BinaryExpression)orExpr.Right;
		rightExpr.Operator.Should().Be(BinaryOperator.Equal);
		rightExpr.Left.Should().BeOfType<PropertyExpression>();
		((PropertyExpression)rightExpr.Left).PropertyPath.Should().Be("c.name");
		rightExpr.Right.Should().BeOfType<ConstantExpression>();
		((ConstantExpression)rightExpr.Right).Value.Should().Be("Bob");
	}

	[Fact]
	public void ShouldParseComplexLogicalExpression()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE (c.age > 20 AND c.name = 'Alice') OR (c.age < 18 AND NOT IS_NULL(c.guardian))";

		// Act
		var result = _parser.Parse(sql);
		output.WriteLine(_parser.DumpDebugInfo(sql));

		// Assert
		result.Should().NotBeNull();
		result.WhereExpression.Should().NotBeNull();
		result.WhereExpression.Should().BeOfType<BinaryExpression>();

		// Top level should be an OR expression
		var orExpr = (BinaryExpression)result.WhereExpression;
		orExpr.Operator.Should().Be(BinaryOperator.Or);

		// Left of OR should be (c.age > 20 AND c.name = 'Alice')
		orExpr.Left.Should().BeOfType<BinaryExpression>();
		var leftAndExpr = (BinaryExpression)orExpr.Left;
		leftAndExpr.Operator.Should().Be(BinaryOperator.And);

		// Left AND's first part (c.age > 20)
		leftAndExpr.Left.Should().BeOfType<BinaryExpression>();
		var ageExpr = (BinaryExpression)leftAndExpr.Left;
		ageExpr.Operator.Should().Be(BinaryOperator.GreaterThan);
		ageExpr.Left.Should().BeOfType<PropertyExpression>();
		((PropertyExpression)ageExpr.Left).PropertyPath.Should().Be("c.age");

		// Left AND's second part (c.name = 'Alice')
		leftAndExpr.Right.Should().BeOfType<BinaryExpression>();
		var nameExpr = (BinaryExpression)leftAndExpr.Right;
		nameExpr.Operator.Should().Be(BinaryOperator.Equal);
		nameExpr.Left.Should().BeOfType<PropertyExpression>();
		((PropertyExpression)nameExpr.Left).PropertyPath.Should().Be("c.name");
		nameExpr.Right.Should().BeOfType<ConstantExpression>();
		((ConstantExpression)nameExpr.Right).Value.Should().Be("Alice");

		// Right of OR should be (c.age < 18 AND NOT IS_NULL(c.guardian))
		orExpr.Right.Should().BeOfType<BinaryExpression>();
		var rightAndExpr = (BinaryExpression)orExpr.Right;
		rightAndExpr.Operator.Should().Be(BinaryOperator.And);

		// Right AND's first part (c.age < 18)
		rightAndExpr.Left.Should().BeOfType<BinaryExpression>();
		var ageExpr2 = (BinaryExpression)rightAndExpr.Left;
		ageExpr2.Operator.Should().Be(BinaryOperator.LessThan);
		ageExpr2.Left.Should().BeOfType<PropertyExpression>();
		((PropertyExpression)ageExpr2.Left).PropertyPath.Should().Be("c.age");

		// Right AND's second part (NOT IS_NULL(c.guardian))
		rightAndExpr.Right.Should().BeOfType<UnaryExpression>();
		var notExpr = (UnaryExpression)rightAndExpr.Right;
		notExpr.Operator.Should().Be(UnaryOperator.Not);
		notExpr.Operand.Should().BeOfType<FunctionCallExpression>();
		var funcExpr = (FunctionCallExpression)notExpr.Operand;
		funcExpr.FunctionName.ToUpperInvariant().Should().Be("IS_NULL");
		funcExpr.Arguments.Should().HaveCount(1);
		funcExpr.Arguments[0].Should().BeOfType<PropertyExpression>();
		((PropertyExpression)funcExpr.Arguments[0]).PropertyPath.Should().Be("c.guardian");
	}

	[Fact]
	public void ShouldParseTopClause()
	{
		// Arrange
		var sql = "SELECT TOP 1 c.ppmEntityID FROM c WHERE c.id = @id";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.Should().NotBeNull();
		result.PropertyPaths.Should().HaveCount(1);
		result.PropertyPaths.Should().Contain("c.ppmEntityID");
		result.FromName.Should().Be("c");
		result.TopValue.Should().Be(1);

		// Check WHERE condition
		result.WhereConditions.Should().HaveCount(1);
		result.WhereConditions[0].PropertyPath.Should().Be("c.id");
		result.WhereConditions[0].Operator.Should().Be(ComparisonOperator.Equals);
	}
}
