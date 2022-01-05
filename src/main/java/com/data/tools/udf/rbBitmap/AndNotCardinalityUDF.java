package com.data.tools.udf.rbBitmap;

import com.data.tools.util.RoaringBitmapUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * hive
 * add jar /opt/jobs/bitmap-hive-udf.jar;
 * create temporary function rb_andnot_cardinality as 'com.data.tools.udf.rbBitmap.AndNotCardinalityUDF';
 */
public class AndNotCardinalityUDF extends UDF implements java.io.Serializable {
    public int evaluate(String rb1, String rb2) {
        return RoaringBitmapUtils.andNotCardinality(rb1, rb2);
    }
}
