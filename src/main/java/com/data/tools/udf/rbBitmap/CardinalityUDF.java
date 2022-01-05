package com.data.tools.udf.rbBitmap;

import com.data.tools.util.RoaringBitmapUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * hive
 * add jar /opt/jobs/bitmap-hive-udf.jar;
 * create temporary function rb_cardinality as 'com.data.tools.udf.rbBitmap.CardinalityUDF';
 */
public class CardinalityUDF extends UDF implements java.io.Serializable {
    public int evaluate(String rb) {
        return RoaringBitmapUtils.cardinality(rb);
    }
}
