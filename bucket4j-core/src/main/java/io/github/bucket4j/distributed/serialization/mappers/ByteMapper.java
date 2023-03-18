package io.github.bucket4j.distributed.serialization.mappers;

import io.github.bucket4j.distributed.proxy.AsyncProxyManager;
import io.github.bucket4j.distributed.proxy.ProxyManager;
import io.github.bucket4j.distributed.proxy.RemoteAsyncBucketBuilder;
import io.github.bucket4j.distributed.proxy.RemoteBucketBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * Mapper interface to serialize keys to {@code byte[]}. Useful for some implementations of {@link ProxyManager}.
 * Contains some serializers for common key types.
 *
 * @param <K> Type of the key to map to serialize as a {@code byte[]}
 * @see ProxyManager#withMapper(Function)
 * @see AsyncProxyManager#withMapper(Function)
 * @see RemoteAsyncBucketBuilder#withMapper(Function)
 * @see RemoteBucketBuilder#withMapper(Function)
 */
@FunctionalInterface
public interface ByteMapper<K> extends Function<K, byte[]> {
    @Override
    default byte[] apply(K k) {
        return toBytes(k);
    }

    /**
     * Maps input to a {@code byte[]}
     * @param k input value to map
     * @return input serialized as {@code byte[]}
     */
    byte[] toBytes(K k);

    /**
     * Mapper that serializes Strings using UTF8 encoding
     *
     * @see String#getBytes(Charset)
     * @see StandardCharsets#UTF_8
     */
    ByteMapper<String> UTF_8 = str -> str.getBytes(StandardCharsets.UTF_8);

    /**
     * Mapper that serializes Strings using UTF8 encoding
     *
     * @see String#getBytes(Charset)
     * @see StandardCharsets#US_ASCII
     */
    ByteMapper<String> ASCII = str -> str.getBytes(StandardCharsets.US_ASCII);

    /**
     * Mapper that serializes Longs using 8 bytes
     */
    ByteMapper<Long> LONG = value -> {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte)(value & 0xFF);
            value >>= 8;
        }
        return result;
    };

    /**
     * Mapper that serializes Integers using 4 bytes
     */
    ByteMapper<Integer> INT = value -> {
        byte[] result = new byte[4];
        for (int i = 3; i >= 0; i--) {
            result[i] = (byte)(value & 0xFF);
            value >>= 8;
        }
        return result;
    };

    /**
     * Mapper that serializes a {@link Serializable} using the inbuilt JDK serializer
     */
    ByteMapper<Object> JDK_SERIALIZATION = obj -> {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutputStream.writeObject(obj);
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) { // ByteArrayOutputStream does not throw IO exceptions
            throw new IllegalStateException(e);
        }
    };

}
