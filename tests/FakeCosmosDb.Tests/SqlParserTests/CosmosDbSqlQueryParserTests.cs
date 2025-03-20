using System.Linq;
using FluentAssertions;
using Newtonsoft.Json.Linq;
using TimAbell.FakeCosmosDb.SqlParser;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests.SqlParserTests;

public class CosmosDbSqlQueryParserTests(ITestOutputHelper output)
{
	private readonly CosmosDbSqlQueryParser _parser = new();

	[Fact]
	public void ShouldParseSimpleSelectQuery()
	{
		// Arrange
		var sql = "SELECT * FROM c";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
	}

	[Fact]
	public void ShouldParseSelectWithProjection()
	{
		// Arrange
		var sql = "SELECT c.id, c.name, c.address.city FROM c";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.Items.Should().HaveCount(3);
		result.SprachedSqlAst.Select.Items.OfType<PropertySelectItem>().Select(i => i.PropertyPath).Should().Contain("c.id");
		result.SprachedSqlAst.Select.Items.OfType<PropertySelectItem>().Select(i => i.PropertyPath).Should().Contain("c.name");
		result.SprachedSqlAst.Select.Items.OfType<PropertySelectItem>().Select(i => i.PropertyPath).Should().Contain("c.address.city");
		result.SprachedSqlAst.From.Source.Should().Be("c");
	}

	[Fact]
	public void ShouldParseWhereWithSimpleCondition()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE c.age > 21";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Where.Should().NotBeNull();

		var whereCondition = result.SprachedSqlAst.Where.Condition as BinaryExpression;
		whereCondition.Should().NotBeNull();
		whereCondition.Operator.Should().Be(BinaryOperator.GreaterThan);

		var leftExpr = whereCondition.Left as PropertyExpression;
		leftExpr.Should().NotBeNull();
		leftExpr.PropertyPath.Should().Be("c.age");

		var rightExpr = whereCondition.Right as ConstantExpression;
		rightExpr.Should().NotBeNull();
		rightExpr.Value.Should().Be(21);
	}

	[Fact]
	public void ShouldParseWhereWithMultipleConditions()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE c.age > 21 AND c.name = 'John'";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Where.Should().NotBeNull();

		var andExpression = result.SprachedSqlAst.Where.Condition as BinaryExpression;
		andExpression.Should().NotBeNull();
		andExpression.Operator.Should().Be(BinaryOperator.And);

		// Check first condition (c.age > 21)
		var leftCondition = andExpression.Left as BinaryExpression;
		leftCondition.Should().NotBeNull();
		leftCondition.Operator.Should().Be(BinaryOperator.GreaterThan);

		var leftProp = leftCondition.Left as PropertyExpression;
		leftProp.Should().NotBeNull();
		leftProp.PropertyPath.Should().Be("c.age");

		var leftValue = leftCondition.Right as ConstantExpression;
		leftValue.Should().NotBeNull();
		leftValue.Value.Should().Be(21);

		// Check second condition (c.name = 'John')
		var rightCondition = andExpression.Right as BinaryExpression;
		rightCondition.Should().NotBeNull();
		rightCondition.Operator.Should().Be(BinaryOperator.Equal);

		var rightProp = rightCondition.Left as PropertyExpression;
		rightProp.Should().NotBeNull();
		rightProp.PropertyPath.Should().Be("c.name");

		var rightValue = rightCondition.Right as ConstantExpression;
		rightValue.Should().NotBeNull();
		rightValue.Value.Should().Be("John");
	}

	[Fact]
	public void ShouldParseOrderBy()
	{
		// Arrange
		var sql = "SELECT * FROM c ORDER BY c.age DESC, c.name ASC";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.OrderBy.Should().NotBeNull();
		result.SprachedSqlAst.OrderBy.Items.Should().HaveCount(2);

		result.SprachedSqlAst.OrderBy.Items[0].PropertyPath.Should().Be("c.age");
		result.SprachedSqlAst.OrderBy.Items[0].Descending.Should().BeTrue();

		result.SprachedSqlAst.OrderBy.Items[1].PropertyPath.Should().Be("c.name");
		result.SprachedSqlAst.OrderBy.Items[1].Descending.Should().BeFalse();
	}

	[Fact]
	public void ShouldParseLimit()
	{
		// Arrange
		var sql = "SELECT * FROM c LIMIT 10";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Limit.Should().NotBeNull();
		result.SprachedSqlAst.Limit.Value.Should().Be(10);
	}

	[Fact]
	public void ShouldParseContainsFunction()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE CONTAINS(c.name, 'John')";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Where.Should().NotBeNull();

		var whereCondition = result.SprachedSqlAst.Where.Condition as FunctionCallExpression;
		whereCondition.Should().NotBeNull();
		whereCondition.FunctionName.Should().Be("CONTAINS");
		whereCondition.Arguments.Should().HaveCount(2);

		var propertyArg = whereCondition.Arguments[0] as PropertyExpression;
		propertyArg.Should().NotBeNull();
		propertyArg.PropertyPath.Should().Be("c.name");

		var valueArg = whereCondition.Arguments[1] as ConstantExpression;
		valueArg.Should().NotBeNull();
		valueArg.Value.Should().Be("John");
	}

	[Fact]
	public void ShouldParseContainsFunctionWithCaseInsensitiveParameter()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE CONTAINS(c.name, 'John', false)";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Where.Should().NotBeNull();

		var whereCondition = result.SprachedSqlAst.Where.Condition as FunctionCallExpression;
		whereCondition.Should().NotBeNull();
		whereCondition.FunctionName.Should().Be("CONTAINS");
		whereCondition.Arguments.Should().HaveCount(3);

		var propertyArg = whereCondition.Arguments[0] as PropertyExpression;
		propertyArg.Should().NotBeNull();
		propertyArg.PropertyPath.Should().Be("c.name");

		var valueArg = whereCondition.Arguments[1] as ConstantExpression;
		valueArg.Should().NotBeNull();
		valueArg.Value.Should().Be("John");

		var ignoreCase = whereCondition.Arguments[2] as ConstantExpression;
		ignoreCase.Should().NotBeNull();
		ignoreCase.Value.Should().Be(false);
	}

	[Fact]
	public void ShouldParseContainsFunctionWithCaseSensitiveParameter()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE CONTAINS(c.name, 'John', true)";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Where.Should().NotBeNull();

		var whereCondition = result.SprachedSqlAst.Where.Condition as FunctionCallExpression;
		whereCondition.Should().NotBeNull();
		whereCondition.FunctionName.Should().Be("CONTAINS");
		whereCondition.Arguments.Should().HaveCount(3);

		var propertyArg = whereCondition.Arguments[0] as PropertyExpression;
		propertyArg.Should().NotBeNull();
		propertyArg.PropertyPath.Should().Be("c.name");

		var valueArg = whereCondition.Arguments[1] as ConstantExpression;
		valueArg.Should().NotBeNull();
		valueArg.Value.Should().Be("John");

		var ignoreCase = whereCondition.Arguments[2] as ConstantExpression;
		ignoreCase.Should().NotBeNull();
		ignoreCase.Value.Should().Be(true);
	}

	[Fact]
	public void ShouldParseStartsWithFunction()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE STARTSWITH(c.name, 'J')";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Where.Should().NotBeNull();

		var whereCondition = result.SprachedSqlAst.Where.Condition as FunctionCallExpression;
		whereCondition.Should().NotBeNull();
		whereCondition.FunctionName.Should().Be("STARTSWITH");
		whereCondition.Arguments.Should().HaveCount(2);

		var propertyArg = whereCondition.Arguments[0] as PropertyExpression;
		propertyArg.Should().NotBeNull();
		propertyArg.PropertyPath.Should().Be("c.name");

		var valueArg = whereCondition.Arguments[1] as ConstantExpression;
		valueArg.Should().NotBeNull();
		valueArg.Value.Should().Be("J");
	}

	[Fact]
	public void ShouldParseBetweenFloatOperator()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE c.age BETWEEN 18.8 AND 65.9";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Where.Should().NotBeNull();

		var binaryExpr = result.SprachedSqlAst.Where.Condition as BinaryExpression;
		binaryExpr.Should().NotBeNull();
		binaryExpr.Operator.Should().Be(BinaryOperator.Between);

		var propertyExpr = binaryExpr.Left as PropertyExpression;
		propertyExpr.Should().NotBeNull();
		propertyExpr.PropertyPath.Should().Be("c.age");

		var betweenExpr = binaryExpr.Right as BetweenExpression;
		betweenExpr.Should().NotBeNull();

		var lowerBound = betweenExpr.LowerBound as ConstantExpression;
		lowerBound.Should().NotBeNull();
		lowerBound.Value.Should().Be(18.8);

		var upperBound = betweenExpr.UpperBound as ConstantExpression;
		upperBound.Should().NotBeNull();
		upperBound.Value.Should().Be(65.9);
	}

	[Fact]
	public void ShouldParseBetweenIntOperator()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE c.age BETWEEN 18 AND 65";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Where.Should().NotBeNull();

		var binaryExpr = result.SprachedSqlAst.Where.Condition as BinaryExpression;
		binaryExpr.Should().NotBeNull();
		binaryExpr.Operator.Should().Be(BinaryOperator.Between);

		var propertyExpr = binaryExpr.Left as PropertyExpression;
		propertyExpr.Should().NotBeNull();
		propertyExpr.PropertyPath.Should().Be("c.age");

		var betweenExpr = binaryExpr.Right as BetweenExpression;
		betweenExpr.Should().NotBeNull();

		var lowerBound = betweenExpr.LowerBound as ConstantExpression;
		lowerBound.Should().NotBeNull();
		lowerBound.Value.Should().Be(18);

		var upperBound = betweenExpr.UpperBound as ConstantExpression;
		upperBound.Should().NotBeNull();
		upperBound.Value.Should().Be(65);
	}

	[Fact]
	public void ShouldParseBetweenStringOperator()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE c.name BETWEEN 'A' AND 'M'";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Where.Should().NotBeNull();

		var binaryExpr = result.SprachedSqlAst.Where.Condition as BinaryExpression;
		binaryExpr.Should().NotBeNull();
		binaryExpr.Operator.Should().Be(BinaryOperator.Between);

		var propertyExpr = binaryExpr.Left as PropertyExpression;
		propertyExpr.Should().NotBeNull();
		propertyExpr.PropertyPath.Should().Be("c.name");

		var betweenExpr = binaryExpr.Right as BetweenExpression;
		betweenExpr.Should().NotBeNull();

		var lowerBound = betweenExpr.LowerBound as ConstantExpression;
		lowerBound.Should().NotBeNull();
		lowerBound.Value.Should().Be("A");

		var upperBound = betweenExpr.UpperBound as ConstantExpression;
		upperBound.Should().NotBeNull();
		upperBound.Value.Should().Be("M");
	}

	[Fact]
	public void ShouldParseCompleteQuery()
	{
		// Arrange
		var sql = "SELECT c.id, c.name, c.age FROM c WHERE c.age > 21 AND CONTAINS(c.name, 'J') ORDER BY c.age DESC LIMIT 10";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.SprachedSqlAst.Should().NotBeNull();

		// Check SELECT
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeFalse();
		result.SprachedSqlAst.Select.Items.Should().HaveCount(3);

		// Verify the property paths in the select items
		var selectItems = result.SprachedSqlAst.Select.Items.Cast<PropertySelectItem>().ToList();
		selectItems[0].PropertyPath.Should().Be("c.id");
		selectItems[1].PropertyPath.Should().Be("c.name");
		selectItems[2].PropertyPath.Should().Be("c.age");

		// Check FROM
		result.SprachedSqlAst.From.Source.Should().Be("c");

		// Check WHERE
		result.SprachedSqlAst.Where.Should().NotBeNull();
		var andExpr = result.SprachedSqlAst.Where.Condition as BinaryExpression;
		andExpr.Should().NotBeNull();
		andExpr.Operator.Should().Be(BinaryOperator.And);

		// Check first condition (c.age > 21)
		var gtExpr = andExpr.Left as BinaryExpression;
		gtExpr.Should().NotBeNull();
		gtExpr.Operator.Should().Be(BinaryOperator.GreaterThan);

		var gtProp = gtExpr.Left as PropertyExpression;
		gtProp.Should().NotBeNull();
		gtProp.PropertyPath.Should().Be("c.age");

		var gtValue = gtExpr.Right as ConstantExpression;
		gtValue.Should().NotBeNull();
		gtValue.Value.Should().Be(21);

		// Check second condition (CONTAINS(c.name, 'J'))
		var containsExpr = andExpr.Right as FunctionCallExpression;
		containsExpr.Should().NotBeNull();
		containsExpr.FunctionName.Should().Be("CONTAINS");
		containsExpr.Arguments.Should().HaveCount(2);

		var containsProp = containsExpr.Arguments[0] as PropertyExpression;
		containsProp.Should().NotBeNull();
		containsProp.PropertyPath.Should().Be("c.name");

		var containsValue = containsExpr.Arguments[1] as ConstantExpression;
		containsValue.Should().NotBeNull();
		containsValue.Value.Should().Be("J");

		// Check ORDER BY
		result.SprachedSqlAst.OrderBy.Should().NotBeNull();
		result.SprachedSqlAst.OrderBy.Items.Should().HaveCount(1);
		result.SprachedSqlAst.OrderBy.Items[0].PropertyPath.Should().Be("c.age");
		result.SprachedSqlAst.OrderBy.Items[0].Descending.Should().BeTrue();

		// Check LIMIT
		result.SprachedSqlAst.Limit.Should().NotBeNull();
		result.SprachedSqlAst.Limit.Value.Should().Be(10);
	}

	[Fact]
	public void ShouldHandleQuotedStringsInWhereConditions()
	{
		// Arrange
		var parser = new CosmosDbSqlQueryParser();
		var sql = "SELECT * FROM c WHERE c.Name = 'Alice'";

		// Act
		var result = parser.Parse(sql);

		// Assert
		result.Should().NotBeNull();
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Where.Should().NotBeNull();

		var binaryExpr = result.SprachedSqlAst.Where.Condition as BinaryExpression;
		binaryExpr.Should().NotBeNull();
		binaryExpr.Operator.Should().Be(BinaryOperator.Equal);

		var propertyExpr = binaryExpr.Left as PropertyExpression;
		propertyExpr.Should().NotBeNull();
		propertyExpr.PropertyPath.Should().Be("c.Name");

		var valueExpr = binaryExpr.Right as ConstantExpression;
		valueExpr.Should().NotBeNull();
		valueExpr.Value.Should().Be("Alice");
	}

	[Fact]
	public void ShouldHandleIntegrationTestQuery()
	{
		// This exact query is used in the integration test
		var parser = new CosmosDbSqlQueryParser();
		var sql = "SELECT * FROM c WHERE c.Name = 'Alice'";

		// Act
		var result = parser.Parse(sql);

		// Output the debug info
		output.WriteLine(CosmosDbSqlQueryParser.DumpDebugInfo(sql));

		// Assert
		result.Should().NotBeNull();
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.From.Alias.Should().BeNull();
		result.SprachedSqlAst.Where.Should().NotBeNull();

		var binaryExpr = result.SprachedSqlAst.Where.Condition as BinaryExpression;
		binaryExpr.Should().NotBeNull();
		binaryExpr.Operator.Should().Be(BinaryOperator.Equal);

		var propertyExpr = binaryExpr.Left as PropertyExpression;
		propertyExpr.Should().NotBeNull();
		propertyExpr.PropertyPath.Should().Be("c.Name");

		var valueExpr = binaryExpr.Right as ConstantExpression;
		valueExpr.Should().NotBeNull();
		valueExpr.Value.Should().Be("Alice");
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
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Where.Should().NotBeNull();

		var isNullExpr = result.SprachedSqlAst.Where.Condition as FunctionCallExpression;
		isNullExpr.Should().NotBeNull();
		isNullExpr.FunctionName.Should().Be("IS_NULL");
		isNullExpr.Arguments.Should().HaveCount(1);

		var propertyArg = isNullExpr.Arguments[0] as PropertyExpression;
		propertyArg.Should().NotBeNull();
		propertyArg.PropertyPath.Should().Be("c.optionalProperty");
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
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Where.Should().NotBeNull();

		var isDefinedExpr = result.SprachedSqlAst.Where.Condition as FunctionCallExpression;
		isDefinedExpr.Should().NotBeNull();
		isDefinedExpr.FunctionName.Should().Be("IS_DEFINED");
		isDefinedExpr.Arguments.Should().HaveCount(1);

		var propertyArg = isDefinedExpr.Arguments[0] as PropertyExpression;
		propertyArg.Should().NotBeNull();
		propertyArg.PropertyPath.Should().Be("c.optionalProperty");
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
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Where.Should().NotBeNull();

		var arrayContainsExpr = result.SprachedSqlAst.Where.Condition as FunctionCallExpression;
		arrayContainsExpr.Should().NotBeNull();
		arrayContainsExpr.FunctionName.Should().Be("ARRAY_CONTAINS");
		arrayContainsExpr.Arguments.Should().HaveCount(2);

		var propertyArg = arrayContainsExpr.Arguments[0] as PropertyExpression;
		propertyArg.Should().NotBeNull();
		propertyArg.PropertyPath.Should().Be("c.tags");

		var valueArg = arrayContainsExpr.Arguments[1] as ConstantExpression;
		valueArg.Should().NotBeNull();
		valueArg.Value.Should().Be("important");
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
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Where.Should().NotBeNull();

		// First AND expression
		var firstAndExpr = result.SprachedSqlAst.Where.Condition as BinaryExpression;
		firstAndExpr.Should().NotBeNull();
		firstAndExpr.Operator.Should().Be(BinaryOperator.And);

		// Second AND expression (left side of first AND)
		var secondAndExpr = firstAndExpr.Left as BinaryExpression;
		secondAndExpr.Should().NotBeNull();
		secondAndExpr.Operator.Should().Be(BinaryOperator.And);

		// IS_DEFINED function (left side of second AND)
		var isDefinedExpr = secondAndExpr.Left as FunctionCallExpression;
		isDefinedExpr.Should().NotBeNull();
		isDefinedExpr.FunctionName.Should().Be("IS_DEFINED");
		isDefinedExpr.Arguments.Should().HaveCount(1);

		var namePropertyExpr = isDefinedExpr.Arguments[0] as PropertyExpression;
		namePropertyExpr.Should().NotBeNull();
		namePropertyExpr.PropertyPath.Should().Be("c.name");

		// ARRAY_CONTAINS function (right side of second AND)
		var arrayContainsExpr = secondAndExpr.Right as FunctionCallExpression;
		arrayContainsExpr.Should().NotBeNull();
		arrayContainsExpr.FunctionName.Should().Be("ARRAY_CONTAINS");
		arrayContainsExpr.Arguments.Should().HaveCount(2);

		var tagsPropertyExpr = arrayContainsExpr.Arguments[0] as PropertyExpression;
		tagsPropertyExpr.Should().NotBeNull();
		tagsPropertyExpr.PropertyPath.Should().Be("c.tags");

		var importantValueExpr = arrayContainsExpr.Arguments[1] as ConstantExpression;
		importantValueExpr.Should().NotBeNull();
		importantValueExpr.Value.Should().Be("important");

		// IS_NULL function (right side of first AND)
		var isNullExpr = firstAndExpr.Right as FunctionCallExpression;
		isNullExpr.Should().NotBeNull();
		isNullExpr.FunctionName.Should().Be("IS_NULL");
		isNullExpr.Arguments.Should().HaveCount(1);

		var deletedAtPropertyExpr = isNullExpr.Arguments[0] as PropertyExpression;
		deletedAtPropertyExpr.Should().NotBeNull();
		deletedAtPropertyExpr.PropertyPath.Should().Be("c.deletedAt");
	}

	[Fact]
	public void ShouldParseNotOperator()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE NOT (c.name = 'Alice')";

		// Act
		var result = _parser.Parse(sql);
		output.WriteLine(CosmosDbSqlQueryParser.DumpDebugInfo(sql));

		// Assert
		result.Should().NotBeNull();
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Where.Should().NotBeNull();

		var unaryExpr = result.SprachedSqlAst.Where.Condition as UnaryExpression;
		unaryExpr.Should().NotBeNull();
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
		output.WriteLine(CosmosDbSqlQueryParser.DumpDebugInfo(sql));

		// Assert
		result.Should().NotBeNull();
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Where.Should().NotBeNull();

		var orExpr = result.SprachedSqlAst.Where.Condition as BinaryExpression;
		orExpr.Should().NotBeNull();
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
		output.WriteLine(CosmosDbSqlQueryParser.DumpDebugInfo(sql));

		// Assert
		result.Should().NotBeNull();
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Where.Should().NotBeNull();

		// Top level should be an OR expression
		var orExpr = result.SprachedSqlAst.Where.Condition as BinaryExpression;
		orExpr.Should().NotBeNull();
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
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeFalse();
		result.SprachedSqlAst.Select.Items.Should().HaveCount(1);
		result.SprachedSqlAst.Select.Items[0].Should().BeOfType<PropertySelectItem>();
		((PropertySelectItem)result.SprachedSqlAst.Select.Items[0]).PropertyPath.Should().Be("c.ppmEntityID");
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Select.Top.Should().NotBeNull();
		result.SprachedSqlAst.Select.Top.Value.Should().Be(1);

		// Check WHERE condition
		result.SprachedSqlAst.Where.Should().NotBeNull();
		result.SprachedSqlAst.Where.Condition.Should().BeOfType<BinaryExpression>();
		var binaryExpr = (BinaryExpression)result.SprachedSqlAst.Where.Condition;
		binaryExpr.Operator.Should().Be(BinaryOperator.Equal);
		binaryExpr.Left.Should().BeOfType<PropertyExpression>();
		((PropertyExpression)binaryExpr.Left).PropertyPath.Should().Be("c.id");
		binaryExpr.Right.Should().BeOfType<ParameterExpression>();
		((ParameterExpression)binaryExpr.Right).ParameterName.Should().Be("id");
	}

	[Fact]
	public void ShouldParseQueryWithParameterInWhereClause()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE c.age > @ageParam";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.Should().NotBeNull();
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Where.Should().NotBeNull();
		result.SprachedSqlAst.Where.Condition.Should().BeOfType<BinaryExpression>();

		var binaryExpression = (BinaryExpression)result.SprachedSqlAst.Where.Condition;
		binaryExpression.Operator.Should().Be(BinaryOperator.GreaterThan);
		binaryExpression.Left.Should().BeOfType<PropertyExpression>();
		((PropertyExpression)binaryExpression.Left).PropertyPath.Should().Be("c.age");
		binaryExpression.Right.Should().BeOfType<ParameterExpression>();
		((ParameterExpression)binaryExpression.Right).ParameterName.Should().Be("ageParam");
	}

	[Fact]
	public void ShouldParseQueryWithMultipleParametersInWhereClause()
	{
		// Arrange
		var sql = "SELECT * FROM c WHERE c.age > @minAge AND c.age <= @maxAge";

		// Act
		var result = _parser.Parse(sql);

		// Assert
		result.Should().NotBeNull();
		result.SprachedSqlAst.Should().NotBeNull();
		result.SprachedSqlAst.Select.IsSelectAll.Should().BeTrue();
		result.SprachedSqlAst.From.Source.Should().Be("c");
		result.SprachedSqlAst.Where.Should().NotBeNull();
		result.SprachedSqlAst.Where.Condition.Should().BeOfType<BinaryExpression>();

		var andExpression = (BinaryExpression)result.SprachedSqlAst.Where.Condition;
		andExpression.Operator.Should().Be(BinaryOperator.And);

		var leftExpression = (BinaryExpression)andExpression.Left;
		leftExpression.Operator.Should().Be(BinaryOperator.GreaterThan);
		leftExpression.Left.Should().BeOfType<PropertyExpression>();
		((PropertyExpression)leftExpression.Left).PropertyPath.Should().Be("c.age");
		leftExpression.Right.Should().BeOfType<ParameterExpression>();
		((ParameterExpression)leftExpression.Right).ParameterName.Should().Be("minAge");

		var rightExpression = (BinaryExpression)andExpression.Right;
		rightExpression.Operator.Should().Be(BinaryOperator.LessThanOrEqual);
		rightExpression.Left.Should().BeOfType<PropertyExpression>();
		((PropertyExpression)rightExpression.Left).PropertyPath.Should().Be("c.age");
		rightExpression.Right.Should().BeOfType<ParameterExpression>();
		((ParameterExpression)rightExpression.Right).ParameterName.Should().Be("maxAge");
	}
}
