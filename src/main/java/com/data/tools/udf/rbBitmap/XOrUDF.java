package com.data.tools.udf.rbBitmap;

import com.data.tools.util.RoaringBitmapUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * hive
 * add jar /opt/jobs/bitmap-hive-udf.jar;
 * create temporary function rb_xor as 'com.data.tools.udf.rbBitmap.XOrUDF';
 */
public class XOrUDF extends UDF implements java.io.Serializable {
    public String evaluate(String rb1, String rb2) {
        return RoaringBitmapUtils.xOr(rb1, rb2);
    }
}