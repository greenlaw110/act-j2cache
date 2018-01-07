package com.greenlaw110.osgl.j2cache;

import net.oschina.j2cache.CacheChannel;
import net.oschina.j2cache.CacheObject;
import net.oschina.j2cache.J2Cache;
import net.oschina.j2cache.util.SerializationUtils;
import org.osgl.$;
import org.osgl.cache.CacheService;
import org.osgl.util.Charsets;
import org.osgl.util.E;
import org.osgl.util.S;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

public class J2CacheService implements CacheService {

    private static final String FLAG_STR = "\0红s";
    private static final String FLAG_LNG = "\0l";
    // corresponding to FLAG_STR
    private static final byte[] PATCH_STR = {0, -25, -70, -94, 115};
    // corresponding to "\0红o"
    private static final byte[] PATCH_OBJ = {0, -25, -70, -94, 111};

    private static final int HC_STR = String.class.hashCode();
    private static final int HC_INTW = Integer.class.hashCode();
    private static final int HC_LNGW = Long.class.hashCode();

    private int defaultTTL = 60;

    private CacheChannel j2cache;
    private String region;

    J2CacheService(String name) {
        j2cache = J2Cache.getChannel();
        region = name;
    }

    @Override
    public void put(String key, Object value, int ttl) {
        if (null == value) {
            evict(key);
            return;
        }
        int hc = value.getClass().hashCode();
        if (HC_STR == hc) {
            value = S.concat(value, FLAG_STR);
        } else if (HC_INTW == hc) {
            // do need to do anything
        } else if (HC_LNGW == hc) {
            // we need to keep long type info to distinct it from int
            try {
                j2cache.set(region, longTypeKey(key), 0, ttl);
                return;
            } catch (IOException e) {
                throw E.ioException(e);
            }
        } else {
            try {
                byte[] ba = SerializationUtils.serialize((Serializable) value);
                byte[] ba2 = $.concat(ba, PATCH_OBJ);
                j2cache.set(region, key, ba2, ttl);
                return;
            } catch (IOException e) {
                throw E.ioException(e);
            }
        }
        // now we add string or int, long type value into cache
        try {
            j2cache.set(region, key, (Serializable) value, ttl);
        } catch (IOException e) {
            throw E.ioException(e);
        }
    }

    @Override
    public void put(String key, Object value) {
        put(key, value, -1);
    }

    @Override
    public void evict(String key) {
        try {
            j2cache.evict(region, key);
        } catch (IOException e) {
            throw E.ioException(e);
        }
    }

    @Override
    public <T> T get(String key) {
        try {
            CacheObject<byte[]> val = j2cache.getBytes(region, key);
            if (null == val) {
                return null;
            }
            byte[] ba = val.getValue();
            int len = ba.length;
            if (isStrPatched(ba, len)) {
                byte[] ba2 = new byte[len - 5];
                System.arraycopy(ba, 0, ba2, 0, len - 5);
                return (T) new String(ba2);
            } else if (isObjPatched(ba, len)) {
                byte[] ba2 = new byte[len - 5];
                System.arraycopy(ba, 0, ba2, 0, len - 5);
                return (T) SerializationUtils.deserialize(ba2);
            }
            String s = new String(ba);
            if (j2cache.exists(region, longTypeKey(key))) {
                return (T)Long.valueOf(s);
            } else {
                return (T) Integer.valueOf(s);
            }
        } catch (IOException e) {
            throw E.ioException(e);
        }
    }

    @Override
    public int incr(String key) {
        try {
            return (int) j2cache.incr(region, key, 0);
        } catch (IOException e) {
            throw E.ioException(e);
        }
    }

    @Override
    public int incr(String key, int ttl) {
        try {
            return (int) j2cache.incr(region, key, ttl);
        } catch (IOException e) {
            throw E.ioException(e);
        }
    }

    @Override
    public int decr(String key) {
        try {
            return (int) j2cache.decr(region, key, 0);
        } catch (IOException e) {
            throw E.ioException(e);
        }
    }

    @Override
    public int decr(String key, int ttl) {
        try {
            return (int) j2cache.decr(region, key, ttl);
        } catch (IOException e) {
            throw E.ioException(e);
        }
    }

    @Override
    public void clear() {
        try {
            j2cache.clear(region);
        } catch (IOException e) {
            // ignore
        }
    }

    @Override
    public void setDefaultTTL(int ttl) {
        defaultTTL = ttl;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void startup() {
    }

    private static boolean isStrPatched(byte[] ba, int len) {
        return isEndWith(ba, PATCH_STR, len);
    }

    private static boolean isObjPatched(byte[] ba, int len) {
        return isEndWith(ba, PATCH_OBJ, len);
    }

    private static boolean isEndWith(byte[] ba, byte[] suffix, int len) {
        if (len < 6) {
            // our patch array has 5 elements
            return false;
        }
        if (ba[len - 1] != suffix[5]) {
            return false;
        }
        if (ba[len - 2] != suffix[4]) {
            return false;
        }
        if (ba[len - 3] != suffix[3]) {
            return false;
        }
        if (ba[len - 4] != suffix[2]) {
            return false;
        }
        if (ba[len - 5] != suffix[1]) {
            return false;
        }
        if (ba[len - 6] != suffix[0]) {
            return false;
        }
        return true;
    }

    private static String longTypeKey(String key) {
        return S.concat(key, FLAG_LNG);
    }

    public static void main(String[] args) {
        byte[] ba = FLAG_STR.getBytes(Charsets.UTF_8);
        System.out.println(Arrays.toString(ba));
    }
}
