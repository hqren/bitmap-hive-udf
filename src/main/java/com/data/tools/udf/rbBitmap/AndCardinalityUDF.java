package com.data.tools.udf.rbBitmap;

import com.data.tools.util.RoaringBitmapUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * hive
 * add jar /opt/jobs/bitmap-hive-udf.jar;
 * create temporary function rb_and_cardinality as 'com.data.tools.udf.rbBitmap.AndCardinalityUDF';
 */
public class AndCardinalityUDF extends UDF implements java.io.Serializable {
    public int evaluate(String rb1, String rb2) {
        return RoaringBitmapUtils.andCardinality(rb1, rb2);
    }
}
