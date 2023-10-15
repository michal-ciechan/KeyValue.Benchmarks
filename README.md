


# Results

## IXWVDEV201

```
BenchmarkDotNet v0.13.9+228a464e8be6c580ad9408e98f18813f6407fb5a, Windows 10 (10.0.19043.1708/21H1/May2021Update) (VMware)
AMD EPYC 74F3, 1 CPU, 8 logical and 8 physical cores
.NET SDK 8.0.100-rc.1.23455.8
  [Host]     : .NET 7.0.11 (7.0.1123.42427), X64 RyuJIT AVX2
  DefaultJob : .NET 7.0.11 (7.0.1123.42427), X64 RyuJIT AVX2
```


| Method        | ParallelIterationCount | Store                | Mean         | Error      | StdDev     |
|-------------- |----------------------- |--------------------- |-------------:|-----------:|-----------:|
| ParallelAsync | 100000                 | Postgres             | 12,764.24 ms | 205.451 ms | 281.224 ms |
| ParallelAsync | 100000                 | Lightning            |    151.71 ms |   2.992 ms |   3.784 ms |
| ParallelAsync | 100000                 | FasterKV             |  4,779.14 ms | 171.764 ms | 506.451 ms |
| ParallelAsync | 100000                 | FasterKVSerialiser   |  4,289.06 ms | 194.141 ms | 572.428 ms |
| ParallelAsync | 100000                 | FasterKVNoCommit     |  1,639.08 ms | 184.766 ms | 544.786 ms |
| ParallelAsync | 100000                 | FasterKVSpanByte     |    325.72 ms |  13.397 ms |  38.221 ms |
| ParallelAsync | 100000                 | ConcurrentDictionary |     96.89 ms |   1.544 ms |   1.289 ms |
