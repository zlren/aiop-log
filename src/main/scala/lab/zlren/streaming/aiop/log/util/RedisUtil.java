package lab.zlren.streaming.aiop.log.util;

import lab.zlren.streaming.aiop.log.entity.PlatformLog;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * 操作redis工具类
 *
 * @author zlren
 * @date 2018-01-18
 */
public class RedisUtil implements Serializable {

    private static String TOTAL_ABILITY = "aiop:log:total:ability";
    private static String TOTAL_RESULT = "aiop:log:total:result";
    private static String INVOKE = "INVOKE";
    private static String SUCCESS = "SUCCESS";
    private static String CACHE_HIT = "CACHE_HIT";

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

    public void hashIncr(String key, String field) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.hincrBy(key, field, 1);
        }
    }

    /**
     * 有序集合
     * 为key这个有序集合里面的member对应的score加一，如果member不存在就会自动创建
     *
     * @param key
     * @param member
     */
    public void sortedSetIncr(String key, String member) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.zincrby(key, 1, member);
        }
    }


    // total

    /**
     * 默认key为aiop:log:total:ability
     *
     * @param abilityName
     */
    public void sortedSetTotalAbilityIncr(String abilityName) {
        sortedSetIncr(TOTAL_ABILITY, abilityName);
    }

    private void abilityResultTotal(String abilityName, String result) {
        hashIncr(TOTAL_RESULT, abilityName + ":" + result);
    }

    private void abilityFailedReasonTotal(String abilityName, String reason) {
        hashIncr(TOTAL_RESULT, abilityName + ":failed:" + reason);
    }

    private void abilityCacheHitTotal(String abilityName) {
        hashIncr(TOTAL_RESULT, abilityName + ":hit");
    }

    private void authFailedTotal(String authType) {
        hashIncr(TOTAL_RESULT, authType + ":failed");
    }

    private void levelCountTotal(String level) {
        hashIncr(TOTAL_RESULT, "level_count:" + level);
    }

    // public void costTotal(String abilityName) {
    //
    // }

    public void processTotal(PlatformLog platformLog) {

        levelCountTotal(platformLog.level());

        if (platformLog.domainNlpAbility().length() > 0) {

            String action = platformLog.domainNlpAction();
            String abilityName = platformLog.domainNlpAbility();

            // NLP
            if (INVOKE.equals(action)) {
                // 这是一次调用

                // 调用次数统计
                sortedSetTotalAbilityIncr(abilityName);
                // 调用结果统计
                abilityResultTotal(abilityName, platformLog.domainNlpResult());
                // 失败原因统计
                if (!SUCCESS.equals(platformLog.domainNlpResult())) {
                    abilityFailedReasonTotal(abilityName, platformLog.domainNlpResult());
                }
            } else if (CACHE_HIT.equals(action) && platformLog.domainNlpResult().equals(SUCCESS)) {
                abilityCacheHitTotal(abilityName);
            }
        } else {
            // OAUTH 失败统计
            if (!platformLog.domainAuthResult().equals(SUCCESS)) {
                authFailedTotal(platformLog.domainAuthType());
            }
        }
    }

    public static void main(String[] args) {
        RedisUtil redisUtil = new RedisUtil();
        redisUtil.sortedSetIncr("aiop:log:total", "zlren");
    }
}
