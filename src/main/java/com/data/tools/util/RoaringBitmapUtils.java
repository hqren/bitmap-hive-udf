package com.data.tools.util;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.nio.ByteBuffer;

public class RoaringBitmapUtils {
    public static ImmutableRoaringBitmap str2Bitmap(String str) {
        if (StringUtils.isEmpty(str)) {
            return null;
        }

        Base64 base64 = new Base64();

        return new ImmutableRoaringBitmap(ByteBuffer.wrap(base64.decode(str)));
    }

    public static String bitmap2Str(ImmutableRoaringBitmap bitmap) {
        if (bitmap == null) {
            return null;
        }

        Base64 base64 = new Base64();
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[bitmap.serializedSizeInBytes()]);
        bitmap.serialize(byteBuffer);

        return base64.encodeToString(byteBuffer.array());
    }

    public static int andCardinality(String rb1, String rb2) {
        ImmutableRoaringBitmap x1 = str2Bitmap(rb1);
        ImmutableRoaringBitmap x2 = str2Bitmap(rb2);

        if (x1 == null) {
            return cardinality(rb2);
        } else if (x2 == null) {
            return cardinality(rb1);
        }

        return ImmutableRoaringBitmap.and(x1, x2).getCardinality();
    }

    public static String and(String rb1, String rb2) {
        ImmutableRoaringBitmap x1 = str2Bitmap(rb1);
        ImmutableRoaringBitmap x2 = str2Bitmap(rb2);

        if (x1 == null) {
            return rb2;
        } else if (x2 == null) {
            return rb1;
        }

        MutableRoaringBitmap x3 = ImmutableRoaringBitmap.and(x1, x2);
        x3.runOptimize();

        return bitmap2Str(x3);
    }

    public static int andNotCardinality(String rb1, String rb2) {
        ImmutableRoaringBitmap x1 = str2Bitmap(rb1);
        ImmutableRoaringBitmap x2 = str2Bitmap(rb2);

        return ImmutableRoaringBitmap.andNot(x1, x2).getCardinality();
    }

    public static String andNot(String rb1, String rb2) {
        ImmutableRoaringBitmap x1 = str2Bitmap(rb1);
        ImmutableRoaringBitmap x2 = str2Bitmap(rb2);

        MutableRoaringBitmap x3 = ImmutableRoaringBitmap.andNot(x1, x2);
        x3.runOptimize();

        return bitmap2Str(x3);
    }

    public static int orCardinality(String rb1, String rb2) {
        if (StringUtils.isEmpty(rb1)) {
            return cardinality(rb2);
        } else if (StringUtils.isEmpty(rb2)) {
            return cardinality(rb1);
        }

        ImmutableRoaringBitmap x1 = str2Bitmap(rb1);
        ImmutableRoaringBitmap x2 = str2Bitmap(rb2);

        if (x1 == null) {
            return cardinality(rb2);
        } else if (x2 == null) {
            return cardinality(rb1);
        }

        return ImmutableRoaringBitmap.or(x1, x2).getCardinality();
    }

    public static String or(String rb1, String rb2) {
        ImmutableRoaringBitmap x1 = str2Bitmap(rb1);
        ImmutableRoaringBitmap x2 = str2Bitmap(rb2);

        if (x1 == null) {
            return rb2;
        } else if (x2 == null) {
            return rb1;
        }

        MutableRoaringBitmap x3 = ImmutableRoaringBitmap.or(x1, x2);
        x3.runOptimize();

        return bitmap2Str(x3);
    }

    public static int xOrCardinality(String rb1, String rb2) {
        ImmutableRoaringBitmap x1 = str2Bitmap(rb1);
        ImmutableRoaringBitmap x2 = str2Bitmap(rb2);

        return ImmutableRoaringBitmap.xor(x1, x2).getCardinality();
    }

    public static String xOr(String rb1, String rb2) {
        ImmutableRoaringBitmap x1 = str2Bitmap(rb1);
        ImmutableRoaringBitmap x2 = str2Bitmap(rb2);

        MutableRoaringBitmap x3 = ImmutableRoaringBitmap.xor(x1, x2);
        x3.runOptimize();

        return bitmap2Str(x3);
    }

    public static int cardinality(String rb) {
        if (StringUtils.isEmpty(rb)) {
            return 0;
        }

        ImmutableRoaringBitmap bitmap = str2Bitmap(rb);
        if (bitmap == null) {
            return 0;
        }


        return bitmap.getCardinality();
    }
}
