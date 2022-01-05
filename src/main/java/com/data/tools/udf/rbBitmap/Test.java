package com.data.tools.udf.rbBitmap;

import com.data.tools.util.RoaringBitmapUtils;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.nio.ByteBuffer;

public class Test {
    public static void main(String[] args) {
        ImmutableRoaringBitmap r1 = ImmutableRoaringBitmap.bitmapOf(1, 2, 3, 4, 5);
        ImmutableRoaringBitmap r2 = ImmutableRoaringBitmap.bitmapOf(3, 4, 5, 6, 7);
        ImmutableRoaringBitmap r3 = ImmutableRoaringBitmap.bitmapOf(7, 11, 12, 15);

        ByteBuffer b1 = ByteBuffer.wrap(new byte[r1.serializedSizeInBytes()]);
        ByteBuffer b2 = ByteBuffer.wrap(new byte[r2.serializedSizeInBytes()]);
        ByteBuffer b3 = ByteBuffer.wrap(new byte[r3.serializedSizeInBytes()]);

        r1.serialize(b1);
        r2.serialize(b2);
        r3.serialize(b3);

        ImmutableRoaringBitmap s = ImmutableRoaringBitmap.or(r1, r2);
        ImmutableRoaringBitmap s2 = ImmutableRoaringBitmap.or(s, r3);
        System.out.println(s2);
        System.out.println(RoaringBitmapUtils.bitmap2Str(s2));
        System.out.println(RoaringBitmapUtils.str2Bitmap("OzAAAAEAAAkAAwABAAYACwABAA8AAAA="));
    }
}
