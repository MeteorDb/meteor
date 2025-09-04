# Meteor Commands Documentation

This document provides comprehensive documentation for the Meteor database commands: RGET, SCAN, and COUNT. Each command supports various syntax patterns and operators.

## RGET (Range GET)

The RGET command retrieves all key-value pairs within a specified key range.

### Syntax
```
RGET startKey endKey [transactionId]
```

### Parameters
- **startKey** (required): The starting key of the range (inclusive)
- **endKey** (required): The ending key of the range (inclusive)
- **transactionId** (optional): The transaction ID for the operation

### Examples

#### Basic Range Queries
```bash
RGET user_a user_z
RGET session_001 session_999
```

#### With Transaction ID
```bash
RGET user_a user_z 12345
RGET session_001 session_999 67890
```

### Constraints
- `startKey` must be lexicographically less than or equal to `endKey`

### Return Value
Returns a JSON object containing all key-value pairs in the specified range.

---

## SCAN

The SCAN command retrieves records from the database based on specified conditions with support for both key and value filtering.

### Syntax
```
SCAN condition [transactionId]
```

### Parameters
- **condition** (required): The filtering condition or `*` for all records
- **transactionId** (optional): The transaction ID for the operation

### Condition Syntax

Conditions support both key and value filtering with logical operators. The `WHERE` prefix is optional.

#### Fields
- **$key** or **key**: Filter on the record key
- **$value** or **value**: Filter on the stored value

#### Operators
- `=` or `==`: Exact equality
- `!=`: Not equal
- `>`: Greater than (numeric for numbers, lexicographic for strings)
- `<`: Less than (numeric for numbers, lexicographic for strings) 
- `>=`: Greater than or equal
- `<=`: Less than or equal
- `LIKE`: Pattern matching with `%` wildcards

#### Logical Operators
- `AND`: Logical AND
- `OR`: Logical OR
- `NOT`: Logical NOT
- `()`: Parentheses for grouping

#### Special Conditions
- `*`: Returns all records in the database

### Examples

#### Get All Records
```bash
SCAN *
```

#### Key Filtering
```bash
# Exact key matching
SCAN "$key = 'user123'"
SCAN "key == 'session_abc'"

# Key comparisons
SCAN "$key > 'user_a'"
SCAN "key < 'user_z'"
SCAN "$key >= 'session_001'"

# Key pattern matching with LIKE
SCAN "$key LIKE 'user_%'"
SCAN "key LIKE '%_cache'"
SCAN "$key LIKE '%temp%'"
```

#### Value Filtering
```bash
# Numeric value comparisons
SCAN $value > 100
SCAN "value < 50"
SCAN "$value >= 200"
SCAN "value <= 1000"

# String value comparisons
SCAN "$value = 'active'"
SCAN "value != 'expired'"
SCAN "$value == 'processed'"

# Value pattern matching
SCAN "$value LIKE '%temp%'"
SCAN "value LIKE 'session_%'"
```

#### Complex Conditions with Logical Operators
```bash
# AND conditions
SCAN "$key LIKE 'user_%' AND $value > 100"
SCAN "key = 'admin' AND value = 'active'"

# OR conditions
SCAN "$key = 'admin' OR $key = 'root'"
SCAN "$value = 'active' OR $value = 'pending'"

# NOT conditions
SCAN "NOT $key LIKE 'temp_%'"
SCAN "NOT $value = 'deleted'"

# Complex grouping
SCAN "($key LIKE 'user_%' OR $key LIKE 'admin_%') AND $value > 50"
SCAN "$key = 'session' AND NOT ($value = 'expired' OR $value = 'invalid')"
```

#### Flexible Expression Support
```bash
# Works without spaces
SCAN $key='user123'
SCAN $value>100

# Supports reverse expressions
SCAN "'user123' = $key"
SCAN "100 < $value"
SCAN "'active' == $value"

# With optional WHERE prefix (for backward compatibility)
SCAN "WHERE $key LIKE 'user_%'"
SCAN "WHERE $value > 100 AND $key != 'admin'"
```

#### With Transaction ID
```bash
SCAN * 12345
SCAN "$key LIKE 'user_%'" 67890
SCAN "$value > 100 AND $key != 'temp'" 54321
```

### Return Value
Returns a JSON object containing all key-value pairs matching the specified condition.

---

## COUNT

The COUNT command returns the number of records matching specified conditions. It uses the exact same condition syntax as SCAN but returns only the count instead of the actual records.

### Syntax
```
COUNT condition [transactionId]
```

### Parameters
- **condition** (required): The filtering condition or `*` for all records
- **transactionId** (optional): The transaction ID for the operation

### Condition Syntax

COUNT uses the exact same condition syntax as SCAN. Refer to the SCAN command documentation for complete syntax details.

#### Fields
- **$key** or **key**: Filter on the record key
- **$value** or **value**: Filter on the stored value

#### Operators
- `=` or `==`: Exact equality
- `!=`: Not equal
- `>`: Greater than (numeric for numbers, lexicographic for strings)
- `<`: Less than (numeric for numbers, lexicographic for strings)
- `>=`: Greater than or equal
- `<=`: Less than or equal
- `LIKE`: Pattern matching with `%` wildcards

#### Logical Operators
- `AND`: Logical AND
- `OR`: Logical OR
- `NOT`: Logical NOT
- `()`: Parentheses for grouping

#### Special Conditions
- `*`: Counts all records in the database

### Examples

#### Count All Records
```bash
COUNT *
```

#### Count by Key Conditions
```bash
# Exact key matching
COUNT "$key = 'user123'"
COUNT "key == 'session_abc'"

# Key comparisons
COUNT "$key > 'user_a'"
COUNT "key < 'user_z'"
COUNT "$key >= 'session_001'"

# Key pattern matching
COUNT "$key LIKE 'user_%'"
COUNT "key LIKE '%_cache'"
COUNT "$key LIKE '%temp%'"
```

#### Count by Value Conditions
```bash
# Numeric value comparisons
COUNT "$value > 100"
COUNT "value < 50"
COUNT "$value >= 200"
COUNT "value <= 1000"

# String value comparisons
COUNT "$value = 'active'"
COUNT "value != 'expired'"
COUNT "$value == 'processed'"

# Value pattern matching
COUNT "$value LIKE '%temp%'"
COUNT "value LIKE 'session_%'"
```

#### Count with Complex Conditions
```bash
# AND conditions
COUNT "$key LIKE 'user_%' AND $value > 100"
COUNT "key = 'admin' AND value = 'active'"

# OR conditions
COUNT "$key = 'admin' OR $key = 'root'"
COUNT "$value = 'active' OR $value = 'pending'"

# NOT conditions
COUNT "NOT $key LIKE 'temp_%'"
COUNT "NOT $value = 'deleted'"

# Complex grouping
COUNT "($key LIKE 'user_%' OR $key LIKE 'admin_%') AND $value > 50"
COUNT "$key = 'session' AND NOT ($value = 'expired' OR $value = 'invalid')"
```

#### Flexible Expression Support
```bash
# Works without spaces
COUNT $key='user123'
COUNT $value>100

# Supports reverse expressions
COUNT "'user123' = $key"
COUNT "100 < $value"
COUNT "'active' == $value"

# With optional WHERE prefix (for backward compatibility)
COUNT "WHERE $key LIKE 'user_%'"
COUNT "WHERE $value > 100 AND $key != 'admin'"
```

#### With Transaction ID
```bash
COUNT * 12345
COUNT "$key LIKE 'user_%'" 67890
COUNT "$value > 100 AND $key != 'temp'" 54321
```

### Return Value
Returns an integer representing the count of matching records.

---

## Transaction Support

All commands support optional transaction IDs:
- If no transaction ID is provided, a new transaction is automatically created
- If a transaction ID is provided, the operation is performed within that existing transaction
- Transaction IDs must be valid existing transaction IDs (not new/unused IDs)

## Intelligent Condition Parser

SCAN and COUNT commands use an intelligent condition parser with the following features:

### Expression Flexibility
- **Space-insensitive**: Works with or without spaces (`$key='value'` or `$key = 'value'`)
- **Reverse expressions**: Supports both `$key = 'value'` and `'value' = $key`
- **Operator precedence**: `NOT` has highest precedence, then `AND`, then `OR`
- **Parentheses**: Use `()` for explicit grouping and precedence control

### Field References
- **$key**: Refers to the record key
- **$value**: Refers to the stored value
- **key/value**: Alternative syntax (without $ prefix)

### Wildcard Patterns
The `LIKE` operator supports flexible wildcard patterns:
- `prefix_%`: Matches keys/values starting with "prefix_"
- `%_suffix`: Matches keys/values ending with "_suffix"
- `%contains%`: Matches keys/values containing "contains"
- `exact`: Matches exactly "exact" (no wildcards)

### Automatic Type Detection
- **Numeric comparisons**: Automatically detected for `>`, `<`, `>=`, `<=` operators
- **String fallback**: Falls back to lexicographic comparison if not numeric
- **Mixed comparisons**: `$value > 100` works for both numeric and string values

## General Notes

1. **String Quoting**: Operands can be optionally enclosed in single quotes (`'value'`) or double quotes (`"value"`)
2. **Expression Quoting**: Expressions containing spaces must be enclosed in quotes (e.g. `"key = 'value'"` or `'key = "value"'` or `"key = value AND value = 'value'"`)
3. **Case Sensitivity**: All field names, operators, and logical keywords are case-sensitive
4. **Lexicographic Ordering**: String comparisons use lexicographic (dictionary) ordering
5. **Numeric Values**: Numeric comparisons are attempted first for numeric operators, falling back to string comparison
6. **Wildcards**: The `LIKE` operator supports `%` wildcards for flexible pattern matching
7. **Tombstones**: Deleted entries (tombstones) are automatically excluded from all results
8. **WHERE Optional**: The `WHERE` prefix is optional but supported for backward compatibility

## Error Handling

Commands will return errors for:
- Invalid syntax or missing required parameters
- Invalid transaction IDs
- Malformed condition expressions
- Unsupported field names or operators
- Invalid key ranges (startKey > endKey in RGET)
- Mismatched parentheses in conditions
- Invalid logical operator combinations