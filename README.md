###背景
Bitmap实现的用户行为留存数据在hive也有相应的备份，为了保证impala和hive一致，在用hive建仓库时将二进制数据进行base64编码以字符串存储。

###使用
如果使用hive来实现bitmap查询逻辑，需要添加UDF和UDAF两种自定义函数，具体操作如下：
* 添加jar包和创建函数
add jar /opt/jobs/bitmap-hive-udf.jar;
add jar /opt/jobs/tags/lib/RoaringBitmap-shade-0.8.13.jar;
* 创建函数
create temporary function rb_and_cardinality as 'com.data.tools.udf.rbBitmap.AndCardinalityUDF';
create temporary function rb_or_cardinality as 'com.data.tools.udf.rbBitmap.OrCardinalityUDF';
create temporary function rb_andnot_cardinality as 'com.data.tools.udf.rbBitmap.AndNotCardinalityUDF';
create temporary function rb_xor_cardinality as 'com.data.tools.udf.rbBitmap.XOrCardinalityUDF';
create temporary function rb_cardinality as 'com.data.tools.udf.rbBitmap.CardinalityUDF';
create temporary function rb_describe as 'com.data.tools.udf.rbBitmap.DescribeUDF';
create temporary function rb_and_agg as 'com.data.tools.udfa.AndAggUDFA';
create temporary function rb_or_agg as 'com.data.tools.udfa.OrAggUDFA';
create temporary function rb_xor_agg as 'com.data.tools.udfa.XOrAggUDFA';
* 函数使用
1 rb_cardinality，rb_and_cardinality、rb_or_cardinality、rb_andnot_cardinality、rb_xor_cardinality这五个方法和PG SQL里使用方法一样
2 rb_describe，因为hive中存储的是base64编码后的文本，可读性不好，这个函数是以可读性更高的方式来输出，参数：base64编码后的字符串，最终输出如 {1,2,3... ...}，更多是为了小数据量的调试使用。
3 rb_and_agg、rb_or_agg、rb_andnot_agg、rb_xor_agg，这四个方法和PG SQL使用方法一致，只不过输出的内容是base64编码后的文本
4 PG SQL中的rb_or_cardinality_agg、rb_and_cardinality_agg、rb_xor_cardinality_agg目前没有实现，因为hive Map端小范围合并和Reduce端的最终合并的输入输出函数逻辑要一致，所以目前没有找到输入base64编码后的文本，而最终输出是bitmap的cardinality。现在已实现rb_and_agg、rb_or_agg、rb_xor_agg聚合函数，但输出结果是base64编码后的文本，为了和PG SQL一样的结果，可以使用rb_cardinality，如：rb_cardinality(rb_and_agg("xxx xxx"))，这样就可以达到PG SQL中的rb_or_cardinality_agg功能。
