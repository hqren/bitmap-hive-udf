package com.data.tools.udf.rbBitmap;

import com.data.tools.util.RoaringBitmapUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * hive
 * add jar /opt/jobs/bitmap-hive-udf.jar;
 * create temporary function rb_describe as 'com.data.tools.udf.rbBitmap.DescribeUDF';
 */
public class DescribeUDF extends UDF implements java.io.Serializable {
    public String evaluate(String rb) {
        return RoaringBitmapUtils.str2Bitmap(rb).toString();
    }
}
