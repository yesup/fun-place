/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.yesup.tools;

import com.google.common.io.BaseEncoding;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.commons.codec.digest.DigestUtils;

/**
 *
 * @author jeffye
 */
public final class UniqueId {

    public static String getUniqueString() {
        UUID uuid = UUID.randomUUID();

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());

        return BaseEncoding.base64Url().omitPadding().encode(buffer.array());
    }

    public static String getRandomString() {
        UUID uuid = UUID.randomUUID();

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(uuid.getMostSignificantBits());

        return BaseEncoding.base32Hex().encode(buffer.array()).substring(0, 6);
    }


    public static String shorter(String input, int maxLen) {
        if ( input == null ) {
            return "";
        }
        if ( input.length() <= maxLen ) {
            return input;
        }

        byte[] digest = DigestUtils.md5(input);

        String hash = BaseEncoding.base64().omitPadding().encode(digest);

        if ( hash.length() <= maxLen ) {
            return hash;
        }

        return hash.substring(0, maxLen);
    }

}