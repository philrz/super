### Function

&emsp; **eval** &mdash; turn upstream values into [`from` operator](../operators/from.md) data sources

### Synopsis

```
eval(expr) -> [string]
abs(n: number) -> number
```

### Description

The _abs_ function returns the absolute value of its argument `n`, which
must be a numeric type.

### Examples

Absolute value of a various numbers:
```mdtest-command
echo '1 -1 0 -1.0 -1(int8) 1(uint8) "foo"' | super -z -c 'yield abs(this)' -
```
=>
```mdtest-output
1
1
0
1.
1(int8)
1(uint8)
error({message:"abs: not a number",on:"foo"})
```
