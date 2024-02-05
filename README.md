# hive3-udf
hive4 에 구현된 UDF 를 hive 3 버전에서 동작하도록 빌드하는 버전이다.

https://github.com/apache/hive/blob/branch-4.0/ql/src/java/org/apache/hadoop/hive/ql/exec/FunctionRegistry.java

## 빌드방법

```console
mvn compile package
```

## array 관련 함수

사용 예시 정리
https://ngela.tistory.com/149

```console  
add jar hdfs://<네입서비스명>/user/<유저명>/hive3-udf-<버전>.jar
CREATE TEMPORARY FUNCTION array_distinct as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFArrayDistinct';
CREATE TEMPORARY FUNCTION array_except as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFArrayExcept';
CREATE TEMPORARY FUNCTION array_intersect as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFArrayIntersect';
CREATE TEMPORARY FUNCTION array_join as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFArrayJoin';
CREATE TEMPORARY FUNCTION array_max as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFArrayMax';
CREATE TEMPORARY FUNCTION array_min as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFArrayMin';
CREATE TEMPORARY FUNCTION array_remove as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFArrayRemove';
CREATE TEMPORARY FUNCTION array_slice as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFArraySlice';
CREATE TEMPORARY FUNCTION array_union as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFArrayUnion';

-- hive complex  
CREATE TEMPORARY FUNCTION json_read as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFJsonRead';
CREATE TEMPORARY FUNCTION set_value as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFSetValue';
```