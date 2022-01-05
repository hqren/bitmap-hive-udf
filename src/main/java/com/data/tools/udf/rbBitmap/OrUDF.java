package com.data.tools.udf.rbBitmap;

import com.data.tools.util.RoaringBitmapUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * hive
 * add jar /opt/jobs/bitmap-hive-udf.jar;
 * create temporary function rb_or as 'com.data.tools.udf.rbBitmap.OrUDF';
 */
public class OrUDF extends UDF implements java.io.Serializable {
    public String evaluate(String rb1, String rb2) {
        return RoaringBitmapUtils.or(rb1, rb2);
    }
}