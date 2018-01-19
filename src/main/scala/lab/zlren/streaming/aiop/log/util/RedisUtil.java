package lab.zlren.streaming.aiop.log.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author zlren
 * @date 2018-01-18
 */
public class RedisUtil {

    private static JedisPool jedisPool;

    static {
        try {
            InputStream inputStream = RedisUtil.class.getClassLoader().getResourceAsStream("redis.properties");
            Properties properties = new Properties();
            properties.load(inputStream);

            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(10);
            jedisPoolConfig.setMaxIdle(10);

            String host = properties.getProperty("redis.host");
            Integer port = Integer.valueOf(properties.getProperty("redis.port"));
            String password = properties.getProperty("redis.password");

            jedisPool = new JedisPool(jedisPoolConfig, host, port, 3000, password);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 取值，默认整数类型
     *
     * @param key   key
     * @param field field
     * @return value
     */
    public Integer hashGet(String key, String field) {
        try (Jedis jedis = jedisPool.getResource()) {
            String value = jedis.hget(key, field);
            return value == null ? 0 : Integer.valueOf(value);
        }
    }

    /**
     * 设置值，默认整数类型
     *
     * @param key   key
     * @param field field
     * @param value value
     */
    public void hashSet(String key, String field, Integer value) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.hset(key, field, String.valueOf(value));
        }
    }
}
