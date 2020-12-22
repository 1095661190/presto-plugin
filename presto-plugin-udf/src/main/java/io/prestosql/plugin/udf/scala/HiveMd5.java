package io.prestosql.plugin.udf.scala;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;


import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HiveMd5 {


    private HiveMd5() {
    }


    @Description(value = "use hive md5 , Convert String to md5")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice hive_md5(@SqlType(StandardTypes.VARCHAR) Slice slice) {
        if (slice == null) {
            return null;
        }
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        String md5= evaluate(digest,new Text(slice.toStringUtf8())).toString();

        return  Slices.copiedBuffer((md5 == null) ? "null" : md5, StandardCharsets.UTF_8);

    }


    /**
     * Convert String to md5
     */
    public static Text evaluate(MessageDigest digest, Text n) {
        if (n == null) {
            return null;
        }
        Text result = new Text();
        digest.reset();
        digest.update(n.getBytes(), 0, n.getLength());
        byte[] md5Bytes = digest.digest();
        String md5Hex = Hex.encodeHexString(md5Bytes);

        result.set(md5Hex);
        return result;
    }

    /**
     * Convert bytes to md5
     */
    public Text evaluate(MessageDigest digest,BytesWritable b) {
        if (b == null) {
            return null;
        }
        Text result = new Text();
        digest.reset();
        digest.update(b.getBytes(), 0, b.getLength());
        byte[] md5Bytes = digest.digest();
        String md5Hex = Hex.encodeHexString(md5Bytes);

        result.set(md5Hex);
        return result;
    }



}
