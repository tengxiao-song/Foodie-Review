package com.hmdp.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {

    @Autowired
    private StringRedisTemplate redisTemplate;

    private static final long BEGIN_TIMESTAMP = 1640995200L; // 2022-01-01 00:00:00
    private static final int BITS = 32;

    public long nextId(String keyPrefix) {
        // 获取当前时间戳
        long now = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        long offset = now - BEGIN_TIMESTAMP;
        // 生成序列号
        String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        Long increment = redisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date); // 如果key不存在，会自动创建并设置为0
        return offset << BITS | increment; // 最后format: 0 31位时间戳 32位自增序列号
    }
}
