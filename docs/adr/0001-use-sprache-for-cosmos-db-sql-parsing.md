# 1. Use Sprache for CosmosDB SQL Parsing

Date: 2025-03-14

## Status

Proposed

## Context

The current implementation of CosmosDB SQL parsing in the InMemoryCosmosDbMock project uses string splitting and regular expressions. This approach is brittle, difficult to maintain, and doesn't properly handle the complexity of SQL syntax, particularly for advanced query scenarios. We need a more robust solution that can properly parse the CosmosDB SQL dialect while maintaining good maintainability and performance.

We considered several options:
1. Continue with the current string splitting approach
2. Use a parser combinator library like Sprache
3. Use a parser generator tool like ANTLR
4. Find an existing CosmosDB SQL parser to integrate

### Option 1: Current String Splitting Approach

The existing implementation uses regular expressions and string splitting to parse SQL queries. This approach is simple but has significant limitations:
- Brittle when handling complex queries
- No proper handling of parentheses in complex WHERE clauses
- Limited support for logical operators (AND, OR)
- Difficult to maintain and extend
- Prone to bugs with edge cases

### Option 2: Sprache Parser Combinator

Sprache is a lightweight parser combinator library for C# that allows building parsers using a functional approach:
- Clean, functional style that's easy to read
- Excellent error reporting
- No external tooling or code generation needed
- Seamlessly integrates with C# code
- Can be slower than generated parsers for very complex grammars
- Has a learning curve if you're not familiar with functional programming

### Option 3: ANTLR

ANTLR (ANother Tool for Language Recognition) is a powerful parser generator:
- Industrial-strength parsing that handles complex grammars
- Excellent tooling (syntax highlighting, visualization)
- Better performance than hand-written parsers for very complex grammars
- Well-documented approach used in many large projects
- Requires an additional build step to generate code
- Generated code can be harder to debug
- Steeper learning curve
- More heavyweight than parser combinators

### Option 4: Microsoft's Parser

Interestingly, while Microsoft must have an internal parser for CosmosDB SQL (since they provide the service), they don't expose a public API or library for parsing CosmosDB's SQL dialect. This means we can't simply use their official implementation.

## Decision

We will use the Sprache parser combinator library to implement a proper parser for CosmosDB SQL. We will gradually replace the current string-based parsing implementation with a more robust grammar-based approach.

The decision to use Sprache rather than ANTLR was based on:
1. Simpler integration with the existing codebase
2. No need for additional build steps or tool dependencies
3. The CosmosDB SQL dialect, while having some complexity, is not as complex as a full programming language
4. The ability to incrementally build and test the parser
5. More lightweight approach that better fits the scope of this project

## Consequences

### Positive

- More robust parsing that can handle complex syntax including nested expressions, logical operators, and a wider range of functions
- Better error messages when query syntax is invalid
- More maintainable code with clearly defined grammar rules
- Easier to extend with new SQL features as CosmosDB evolves
- No extra build steps or code generation required
- Lightweight and focused library with minimal dependencies

### Negative

- Requires learning the parser combinator approach, which has a moderate learning curve
- Initial implementation will require more effort than simple string-based parsing
- May introduce some performance overhead compared to the current simplistic approach (though this would be minimal for typical query sizes)
- Requires adding a new dependency to the project

### Implementation Plan

1. Add Sprache as a NuGet package dependency
2. Create a new grammar class defining the CosmosDB SQL syntax using Sprache combinators
3. Define an AST (Abstract Syntax Tree) to represent parsed queries
4. Implement the parser to transform SQL strings into the AST
5. Create a visitor implementation to execute queries against the in-memory store
6. Replace the current string-based parser gradually, starting with the basic SELECT, FROM, WHERE clauses
7. Add support for more complex features like logical operators, nested expressions, and additional functions
8. Ensure comprehensive test coverage for all supported syntax
