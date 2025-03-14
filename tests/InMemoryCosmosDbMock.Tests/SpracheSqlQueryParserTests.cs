using System.Linq;
using FluentAssertions;
using Newtonsoft.Json.Linq;
using TimAbell.MockableCosmos;
using TimAbell.MockableCosmos.Parsing;
using Xunit;

namespace InMemoryCosmosDbMock.Tests;

public class SpracheSqlQueryParserTests
{
    private readonly ICosmosDbQueryParser _parser = new SpracheSqlQueryParser();

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
        result.WhereConditions[0].Operator.Should().Be(">");
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
        result.WhereConditions[0].Operator.Should().Be(">");
        result.WhereConditions[0].Value.Value<int>().Should().Be(21);

        result.WhereConditions[1].PropertyPath.Should().Be("c.name");
        result.WhereConditions[1].Operator.Should().Be("=");
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
        result.OrderBy[0].Direction.Should().Be("DESC");

        result.OrderBy[1].PropertyPath.Should().Be("c.name");
        result.OrderBy[1].Direction.Should().Be("ASC");
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
        result.WhereConditions[0].Operator.Should().Be("CONTAINS");
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
        result.WhereConditions[0].Operator.Should().Be("STARTSWITH");
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
        result.WhereConditions[0].Operator.Should().Be(">");
        result.WhereConditions[0].Value.Value<int>().Should().Be(21);

        result.WhereConditions[1].PropertyPath.Should().Be("c.name");
        result.WhereConditions[1].Operator.Should().Be("CONTAINS");
        result.WhereConditions[1].Value.Value<string>().Should().Be("J");

        result.OrderBy.Should().HaveCount(1);
        result.OrderBy[0].PropertyPath.Should().Be("c.age");
        result.OrderBy[0].Direction.Should().Be("DESC");

        result.Limit.Should().Be(10);
    }

    [Fact]
    public void ShouldHandleQuotedStringsInWhereConditions()
    {
        // Arrange
        var parser = new SpracheSqlQueryParser();
        var sql = "SELECT * FROM c WHERE c.Name = 'Alice'";

        // Act
        var result = parser.Parse(sql);

        // Assert
        result.Should().NotBeNull();
        result.WhereConditions.Should().NotBeNull();
        result.WhereConditions.Should().HaveCount(1);
        result.WhereConditions[0].PropertyPath.Should().Be("c.Name");
        result.WhereConditions[0].Operator.Should().Be("=");
        result.WhereConditions[0].Value.Type.Should().Be(JTokenType.String);
        result.WhereConditions[0].Value.ToString().Should().Be("Alice");
    }
}
