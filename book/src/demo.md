# What is SuperDB?

SuperDB is a new type of analytics database that promises an easier approach
to modern data because it unifies relational tables and eclectic JSON in a
new data model called _super-structured_ data.

Super-structured data is
* self-describing,
* dynamic, and
* strongly typed.

SuperDB takes the best ideas of current data systems and adapts them
for super-structured data with new versions of:

* the data model,
* the serialization formats,
* the query language,
* the query engine, and
* a data lake.

All implemented in a dependency-free command called `super`

To achieve high performance for dynamically type data,
SuperDB has devised a novel vectorized runtime specialized for super-structured data
that is built as a _clean slate_ around [sum types]()
in contrast to the Frankenstein approach taken by other analytics systems that
[shred variants](https://github.com/apache/parquet-format/blob/master/VariantShredding.md#value-shredding)
into relational columns.
This leads to ergonomics for SuperDB that are far better for the query language
and for managing data end to end because there is not one way for handling
relational data and a different way for managing dynamic data.

## What about SQL++

SuperDB shares goals with SQL++

Why doesn't SQL++ solve all these problems?

> The document model isn't _really_ a superset of the relational model,
> and this makes all the difference.

## A Story about Type Checking

Andy Pavlo's [homework](https://15445.courses.cs.cmu.edu/fall2025/homework1/)
using baseball stats

Tried to solve it using SuperSQL
* challenges debugging complex queries
* typos/bugs just returned wrong results
* didn't know where problem was coming from

Wrote in SQL and realized just how important type checks were

Example: [spq query](https://github.com/mccanne/q4/blob/main/q4.spq)

```
cd ~/repo/q4
diff q4-typo.spq q4.spq
super -I q4-typo.spq
super -I q4.spq
```
> It turns out the
> [homework solution](https://15445.courses.cs.cmu.edu/fall2025/files/hw1-sols.tar.gz)
> is actually wrong.  I used SuperSQL to explore the datasets and figure out the bug.

## Document-model and Types

Try this with Couchbase...
```
SELECT x FROM (
  SELECT 1 as x
  UNION ALL
  SELECT 2 as x
  UNION ALL
  SELECT 3 as x
) T
```
What if you have a typo?

Replace `x` with `xx` and you get empty objects?!?!

This is a serious flaw.  Not a big deal here but when a typo of a field name
is deeply buried in a complex query, it can be really difficult to debug.

Here is the fundamental problem...
```
SELECT type(T) FROM (
  SELECT 1 as x
  UNION ALL
  SELECT 2 as x
  UNION ALL
  SELECT 3 as x
) T
```
The type is `object`.

This is not helpful since type analysis
cannot check for valid field-reference without the field names.

## Super-structured Data Model and Types

SuperSQL to the rescue!

```
super -c "
SELECT typeof(T) FROM (
  SELECT 1 as x
  UNION ALL
  SELECT 2 as x
  UNION ALL
  SELECT 3 as x
) T
"
```
Here we know the type is `{x:int64}` so the type checker can actually function.
```
super -c "
SELECT xx FROM (
  SELECT 1 as x
  UNION ALL
  SELECT 2 as x
  UNION ALL
  SELECT 3 as x
) T
"
```

## Types and Column References

There is a similar problem with column references.
SQL++ doesn't know whichside of the join `x` and `y` are
on because the document-model doesn't provide the type info.
```
WITH A AS (
  SELECT 1 AS x
  UNION ALL
  SELECT 2 AS x
  UNION ALL
  SELECT 3 AS x
), B as (
  SELECT 3 AS y
  UNION ALL
  SELECT 4 AS y
)
  -- "x" can't be referenced and is ambiguous?!
SELECT x,y FROM A JOIN B ON x=y
  -- Need explicit table aliases...
--SELECT A.x,B.y FROM A JOIN B ON A.x=B.y
```

Works fine with super-structured data...
```
super -c "
WITH A AS (
  SELECT 1 AS x
  UNION ALL
  SELECT 2 AS x
  UNION ALL
  SELECT 3 AS x
), B as (
  SELECT 3 AS y
  UNION ALL
  SELECT 4 AS y
)
SELECT x,y FROM A JOIN B ON x=y
"
```

## Performance

What about performance?

Let's process some security logs from Zeek

We have some logs in `zeek.bsup`

Let's make a Parquet file for fast processing...

```
cd ~/demo/zeek
super -j zeek.bsup > zeek.json
duckdb -c "copy (from read_json('zeek.json', union_by_name=true)) to zeek.parquet"
```
But this makes a big ugly table...
```
duckdb -json -c "select * from zeek.parquet limit 1" | jq .
```
when the actual first record is this...
```
super -S -c "from zeek.bsup | limit 1"
```
Couchbase doesn't have this problem either!
```
SELECT zeek.*
FROM zeek
LIMIT 1
```
Super Column (CSUP) doesn't have the problem either!
```
super -f csup -o zeek.csup zeek.bsup
super -S -c "from zeek.csup | limit 1"
```
But the big ugly table doesn't matter for many analytics use cases, e.g.,
```
time duckdb -c "SELECT count(),_path FROM zeek.parquet GROUP BY _path"
```
That's fast! 40ms

SuperDB with CSUP is fast too
```
time SUPER_VAM=1 super -c "SELECT count(),_path FROM zeek.csup GROUP BY _path"
```
Also about 40ms.

Try Couchbase
```
SELECT count(*),_path
FROM zeek
GROUP BY _path
```
20 seconds blah!

Again, what if we had a typo?!
```
SELECT count(*),_pathx
FROM zeek
GROUP BY _pathx
```
We get an answer at it's wrong!

> THIS IS REALLY BAD!!!

Compare to SuperSQL...
```
super -c "
SELECT count(*),_pathx
FROM zeek.parquet
GROUP BY _pathx
"
```

## References

### Sum Types

* [Union Types for Semistructured Data](https://homepages.inf.ed.ac.uk/opb/papers/DBPL1999b.pdf)

### Type Fusion / Schema Inference

* [Schema Inference for Massive JSON Datasets](https://openproceedings.org/2017/conf/edbt/paper-62.pdf)

### Couchbase / SQL++

* [SQL++: We Can Finally Relax!](https://escholarship.org/content/qt2bj3m590/qt2bj3m590_noSplash_084218340bb4e928c05878f04d01f04d.pdf)
* [Columnar Formats for Schemaless LSM-based Document Stores](https://arxiv.org/pdf/2111.11517)
* [Cloudy With a Chance of JSON](https://www.vldb.org/pvldb/vol18/p4938-carey.pdf)

#### Basics

```
super -c "values 'hello, world'"
super -c "values this"
echo "1 2 3" | super -c "values this" -
seq 10 | super -c "values {x:this,y:log(this)}" -
seq 10 | super -f parquet -o vals.parquet -c "values {x:this,y:log(this)}" -
duckdb -c "select * from vals.parquet"
super -c "select * from vals.parquet"
```
With Couchbase...
```
SELECT VALUE 'hello, world'
```

#### Pipe Friendliness

```
super -c "from vals.parquet | sum(y)"
super -c "from vals.parquet" | super -c "sum(y)" -
```

#### SuperSQL Column Resolution

```
super -c "

WITH A AS (
  SELECT * FROM (values {x:1},{x:2},{x:3})
),
B as (
  SELECT * FROM (values {y:3},{y:4})
)
SELECT x,y FROM A JOIN B ON x=y

"
```

This works because we do proper type analysis...

```
super -c "

with A as (
  select * from (values {x:1},{x:2},{x:3})
),
B as (
  select * from (values {y:3},{y:4})
)
select typeof(A) from A
-- select typeof(B) from B
"
```

#### Group As

Why have `GROUP AS`?

There is already `array_agg` and a way to get at the table value.

```
SELECT y, g AS TheGroup, array_agg({'A':A}) AS TheList
FROM  [{'x':1,'y':'a'},{'x':2,'y':'b'},{'x':3,'y':'b'}] A
GROUP BY y
GROUP AS g
```

`GROUP AS` in SuperSQL...

```
super -c "

values {'x':1,'y':'a'},{'x':2,'y':'b'},{'x':3,'y':'b'}
| TheList:=collect(this) by y

"
```

### Demo - Security Logs

```
super -S -c "from zeek.csup | head 1"        
```
Compare to relational schema inference... blah!
```
duckdb -json -c "select * from zeek.json limit 1" | jq .
```
Compared to...
```
head -1 zeek.json | jq .
```
Couchbase doesn't have this problem...
```
SELECT zeek.*
FROM zeek
LIMIT 1
```

Let's do a simple aggregation...
```
super -c "from zeek.csup | count() by _path"
```
DuckDB gets the right answer even with all those NULLs
```
duckdb -c "SELECT count(),_path FROM zeek.json GROUP BY _path"
```
We can do SQL too!
```
super -c "SELECT count(),_path FROM zeek.csup GROUP BY _path"
```
Try Couchbase...
```
SELECT count(*),_path
FROM zeek
GROUP BY _path
```
Wow that took a long time
> _I'm sure Capella columnar is much faster!_

DuckDB and SuperDB are columnar... so FAST
```
duckdb -c "COPY (FROM read_json('zeek.json',union_by_name=true)) to 'zeek.parquet'"
time duckdb -c "SELECT count(),_path FROM zeek.parquet GROUP BY _path"
time SUPER_VAM=1 super -c "SELECT count(),_path FROM zeek.csup GROUP BY _path"
```

Here's a fun and non-trival query to mine DNS requests for google servers...

```
super -S -c "
from zeek.bsup 
| _path='dns'
| unnest {query,answer:answers}
| ? google
| answers:=union(answer) by query
| len:=len(answers)
| order by len
"
```
With DuckDB...
```
duckdb -json -c "
SELECT len(answers), answers, query
FROM (
  SELECT array_agg(distinct answer) as answers, query
  FROM (
    SELECT unnest(answers) as answer,query
    FROM zeek.parquet
    WHERE query LIKE '%google%'
  )
  GROUP BY query
)
ORDER BY len(answers)
" | jq .
```

With Couchbase...
```
SELECT len(main.answers), main.answers, main.query
FROM (
  SELECT array_agg(distinct filtered.answer) as answers, filtered.query
  FROM (
    SELECT zeek.query,answer
    FROM zeek
    UNNEST answers as answer
    WHERE zeek.query LIKE '%google%'
  ) filtered
  GROUP BY filtered.query
) main
ORDER BY len(main.answers)
```

### Demo - GitHub Archive

XXX link to duckdb article


