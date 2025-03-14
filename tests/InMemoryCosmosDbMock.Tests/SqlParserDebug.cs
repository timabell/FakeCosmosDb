using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using TimAbell.MockableCosmos;
using Xunit;
using TimAbell.MockableCosmos.Parsing;
using Xunit.Abstractions;

namespace InMemoryCosmosDbMock.Tests
{
    public class SqlParserDebug
    {
        private readonly ITestOutputHelper _output;

        public SqlParserDebug(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void DebugSimpleQuery()
        {
            // Test various queries
            DebugQuery("SELECT * FROM c WHERE c.age > 21 AND c.name = 'John'");
            DebugQuery("SELECT * FROM c ORDER BY c.age DESC");
            DebugQuery("SELECT * FROM c LIMIT 10");
            DebugQuery("SELECT * FROM c WHERE CONTAINS(c.name, 'John')");
        }

        private void DebugQuery(string sql)
        {
            _output.WriteLine("\n========================================");
            _output.WriteLine($"SQL Query: {sql}");
            _output.WriteLine("========================================");

            try
            {
                var ast = CosmosDbSqlGrammar.ParseQuery(sql);
                _output.WriteLine("AST parsed successfully");

                // Debug SELECT clause
                _output.WriteLine($"SELECT clause has {ast.Select.Items.Count} items");
                foreach (var item in ast.Select.Items)
                {
                    _output.WriteLine($"  Item type: {item.GetType().Name}");
                }

                // Debug FROM clause
                _output.WriteLine($"FROM clause: source={ast.From.Source}, alias={ast.From.Alias ?? "null"}");

                // Debug WHERE clause
                if (ast.Where != null)
                {
                    _output.WriteLine($"WHERE clause present, condition type: {ast.Where.Condition.GetType().Name}");
                    DebugExpression(ast.Where.Condition, "  ");
                }
                else
                {
                    _output.WriteLine("WHERE clause not present");
                }

                // Debug ORDER BY clause
                if (ast.OrderBy != null)
                {
                    _output.WriteLine($"ORDER BY clause present with {ast.OrderBy.Items.Count} items");
                    foreach (var item in ast.OrderBy.Items)
                    {
                        _output.WriteLine($"  Property: {item.PropertyPath}, Direction: {(item.Descending ? "DESC" : "ASC")}");
                    }
                }
                else
                {
                    _output.WriteLine("ORDER BY clause not present");
                }

                // Debug LIMIT clause
                if (ast.Limit != null)
                {
                    _output.WriteLine($"LIMIT clause present: {ast.Limit.Value}");
                }
                else
                {
                    _output.WriteLine("LIMIT clause not present");
                }

                // Now convert to ParsedQuery to see what we get
                var parser = new SpracheSqlQueryParser();
                var parsedQuery = parser.Parse(sql);

                _output.WriteLine("\nConverted to ParsedQuery:");
                _output.WriteLine($"  FromName: {parsedQuery.FromName}");
                _output.WriteLine($"  FromAlias: {parsedQuery.FromAlias ?? "null"}");
                _output.WriteLine($"  PropertyPaths: {string.Join(", ", parsedQuery.PropertyPaths)}");
                _output.WriteLine($"  WhereConditions count: {parsedQuery.WhereConditions?.Count ?? 0}");
                if (parsedQuery.WhereConditions != null && parsedQuery.WhereConditions.Count > 0)
                {
                    foreach (var cond in parsedQuery.WhereConditions)
                    {
                        _output.WriteLine($"    {cond.PropertyPath} {cond.Operator} {cond.Value}");
                    }
                }
                _output.WriteLine($"  OrderBy count: {parsedQuery.OrderBy?.Count ?? 0}");
                if (parsedQuery.OrderBy != null && parsedQuery.OrderBy.Count > 0)
                {
                    foreach (var order in parsedQuery.OrderBy)
                    {
                        _output.WriteLine($"    {order.PropertyPath} {order.Direction}");
                    }
                }
                _output.WriteLine($"  Limit: {parsedQuery.Limit}");
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Error parsing: {ex.Message}");
                if (ex.InnerException != null)
                {
                    _output.WriteLine($"Inner exception: {ex.InnerException.Message}");
                }
            }
        }

        private void DebugExpression(Expression expr, string indent)
        {
            if (expr is BinaryExpression binExpr)
            {
                _output.WriteLine($"{indent}BinaryExpression: Operator={binExpr.Operator}");
                _output.WriteLine($"{indent}Left:");
                DebugExpression(binExpr.Left, indent + "  ");
                _output.WriteLine($"{indent}Right:");
                DebugExpression(binExpr.Right, indent + "  ");
            }
            else if (expr is PropertyExpression propExpr)
            {
                _output.WriteLine($"{indent}PropertyExpression: Path={propExpr.PropertyPath}");
            }
            else if (expr is ConstantExpression constExpr)
            {
                _output.WriteLine($"{indent}ConstantExpression: Value={constExpr.Value} ({constExpr.Value?.GetType().Name ?? "null"})");
            }
            else if (expr is FunctionCallExpression funcExpr)
            {
                _output.WriteLine($"{indent}FunctionCallExpression: Name={funcExpr.FunctionName}, Args={funcExpr.Arguments.Count}");
                for (int i = 0; i < funcExpr.Arguments.Count; i++)
                {
                    _output.WriteLine($"{indent}Arg {i}:");
                    DebugExpression(funcExpr.Arguments[i], indent + "  ");
                }
            }
            else
            {
                _output.WriteLine($"{indent}Unknown expression type: {expr.GetType().Name}");
            }
        }

        [Fact]
        public void DebugCompleteQuery()
        {
            var sql = "SELECT c.id, c.name, c.age FROM c WHERE c.age > 21 AND CONTAINS(c.name, 'J') ORDER BY c.age DESC LIMIT 10";
            _output.WriteLine($"SQL Query: {sql}");

            try
            {
                var ast = CosmosDbSqlGrammar.ParseQuery(sql);
                _output.WriteLine("AST parsed successfully");

                // Debug the AST
                _output.WriteLine($"SELECT clause has {ast.Select.Items.Count} items");
                foreach (var item in ast.Select.Items)
                {
                    if (item is PropertySelectItem propItem)
                    {
                        _output.WriteLine($"  Property: {propItem.PropertyPath}");
                    }
                    else
                    {
                        _output.WriteLine($"  Item type: {item.GetType().Name}");
                    }
                }

                _output.WriteLine($"FROM clause: source={ast.From.Source}, alias={ast.From.Alias ?? "null"}");

                if (ast.Where != null)
                {
                    _output.WriteLine($"WHERE clause present, condition type: {ast.Where.Condition.GetType().Name}");
                    DebugExpression(ast.Where.Condition, "  ");
                }
                else
                {
                    _output.WriteLine("WHERE clause not present");
                }

                if (ast.OrderBy != null)
                {
                    _output.WriteLine($"ORDER BY clause present with {ast.OrderBy.Items.Count} items");
                    foreach (var item in ast.OrderBy.Items)
                    {
                        _output.WriteLine($"  Property: {item.PropertyPath}, Direction: {(item.Descending ? "DESC" : "ASC")}");
                    }
                }
                else
                {
                    _output.WriteLine("ORDER BY clause not present");
                }

                if (ast.Limit != null)
                {
                    _output.WriteLine($"LIMIT clause present: {ast.Limit.Value}");
                }
                else
                {
                    _output.WriteLine("LIMIT clause not present");
                }

                // Now convert to ParsedQuery to see what we get
                var parser = new SpracheSqlQueryParser();
                var parsedQuery = parser.Parse(sql);

                _output.WriteLine("\nParsedQuery from SpracheSqlQueryParser.Parse():");
                _output.WriteLine($"  FromName: {parsedQuery.FromName}");
                _output.WriteLine($"  FromAlias: {parsedQuery.FromAlias ?? "null"}");
                _output.WriteLine($"  PropertyPaths: {string.Join(", ", parsedQuery.PropertyPaths)}");
                _output.WriteLine($"  WhereConditions: {(parsedQuery.WhereConditions == null ? "null" : parsedQuery.WhereConditions.Count.ToString())}");
                if (parsedQuery.WhereConditions != null && parsedQuery.WhereConditions.Count > 0)
                {
                    foreach (var cond in parsedQuery.WhereConditions)
                    {
                        _output.WriteLine($"    {cond.PropertyPath} {cond.Operator} {cond.Value}");
                    }
                }
                _output.WriteLine($"  OrderBy: {(parsedQuery.OrderBy == null ? "null" : parsedQuery.OrderBy.Count.ToString())}");
                if (parsedQuery.OrderBy != null && parsedQuery.OrderBy.Count > 0)
                {
                    foreach (var order in parsedQuery.OrderBy)
                    {
                        _output.WriteLine($"    {order.PropertyPath} {order.Direction}");
                    }
                }
                _output.WriteLine($"  Limit: {parsedQuery.Limit}");

                // Now directly construct the ParsedQuery with values we know should be there
                var manualQuery = new ParsedQuery
                {
                    PropertyPaths = new List<string> { "c.id", "c.name", "c.age" },
                    FromName = "c",
                    WhereConditions = new List<WhereCondition>
                    {
                        new WhereCondition
                        {
                            PropertyPath = "c.age",
                            Operator = ">",
                            Value = JToken.FromObject(21)
                        },
                        new WhereCondition
                        {
                            PropertyPath = "c.name",
                            Operator = "CONTAINS",
                            Value = JToken.FromObject("J")
                        }
                    },
                    OrderBy = new List<OrderInfo>
                    {
                        new OrderInfo
                        {
                            PropertyPath = "c.age",
                            Direction = "DESC"
                        }
                    },
                    Limit = 10
                };

                _output.WriteLine("\nManually constructed ParsedQuery:");
                _output.WriteLine($"  FromName: {manualQuery.FromName}");
                _output.WriteLine($"  FromAlias: {manualQuery.FromAlias ?? "null"}");
                _output.WriteLine($"  PropertyPaths: {string.Join(", ", manualQuery.PropertyPaths)}");
                _output.WriteLine($"  WhereConditions: {(manualQuery.WhereConditions == null ? "null" : manualQuery.WhereConditions.Count.ToString())}");
                if (manualQuery.WhereConditions != null && manualQuery.WhereConditions.Count > 0)
                {
                    foreach (var cond in manualQuery.WhereConditions)
                    {
                        _output.WriteLine($"    {cond.PropertyPath} {cond.Operator} {cond.Value}");
                    }
                }
                _output.WriteLine($"  OrderBy: {(manualQuery.OrderBy == null ? "null" : manualQuery.OrderBy.Count.ToString())}");
                if (manualQuery.OrderBy != null && manualQuery.OrderBy.Count > 0)
                {
                    foreach (var order in manualQuery.OrderBy)
                    {
                        _output.WriteLine($"    {order.PropertyPath} {order.Direction}");
                    }
                }
                _output.WriteLine($"  Limit: {manualQuery.Limit}");
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Error parsing: {ex.Message}");
                if (ex.InnerException != null)
                {
                    _output.WriteLine($"Inner exception: {ex.InnerException.Message}");
                }
            }
        }

        [Fact]
        public void DebugOrderByQuery()
        {
            var sql = "SELECT c.id, c.name, c.age FROM c WHERE c.age > 21 AND CONTAINS(c.name, 'J') ORDER BY c.age DESC LIMIT 10";
            _output.WriteLine($"SQL Query: {sql}");

            try
            {
                var ast = CosmosDbSqlGrammar.ParseQuery(sql);
                _output.WriteLine("AST parsed successfully");

                // Debug ORDER BY clause
                if (ast.OrderBy != null)
                {
                    _output.WriteLine($"ORDER BY clause present with {ast.OrderBy.Items.Count} items");
                    foreach (var item in ast.OrderBy.Items)
                    {
                        _output.WriteLine($"  Property: {item.PropertyPath}, Direction: {(item.Descending ? "DESC" : "ASC")}");
                    }
                }
                else
                {
                    _output.WriteLine("ORDER BY clause not present in AST!");
                }

                // Now trace through the OrderBy parsing
                _output.WriteLine("\nTrying to parse just the ORDER BY clause from the query");

                // Break the query into parts
                var parts = sql.Split(new[] { "ORDER BY" }, StringSplitOptions.None);
                if (parts.Length > 1)
                {
                    string orderByClause = "ORDER BY" + parts[1];
                    if (orderByClause.IndexOf("LIMIT", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        orderByClause = orderByClause.Substring(0, orderByClause.IndexOf("LIMIT", StringComparison.OrdinalIgnoreCase));
                    }

                    _output.WriteLine($"Extracted ORDER BY clause: {orderByClause}");

                    // Manually convert to ParsedQuery and see OrderBy result
                    var parser = new SpracheSqlQueryParser();
                    var result = parser.Parse(sql);

                    _output.WriteLine("\nConverted to ParsedQuery:");
                    _output.WriteLine($"  OrderBy is null: {result.OrderBy == null}");
                    if (result.OrderBy != null)
                    {
                        _output.WriteLine($"  OrderBy count: {result.OrderBy.Count}");
                        foreach (var order in result.OrderBy)
                        {
                            _output.WriteLine($"    {order.PropertyPath} {order.Direction}");
                        }
                    }
                }
                else
                {
                    _output.WriteLine("Could not find ORDER BY in query");
                }
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Error parsing: {ex.Message}");
                if (ex.InnerException != null)
                {
                    _output.WriteLine($"Inner exception: {ex.InnerException.Message}");
                }
            }
        }
    }
}
