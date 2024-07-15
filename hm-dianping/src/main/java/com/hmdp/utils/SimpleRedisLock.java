package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{

    private StringRedisTemplate redisTemplate;

    private String name;

    public SimpleRedisLock(StringRedisTemplate redisTemplate, String name) {
        this.redisTemplate = redisTemplate;
        this.name = name;
    }

    private static final String key_prefix = "lock:";

    private static final String ID_PREFIX = UUID.randomUUID().toString(true)+"-";

    private static final DefaultRedisScript<Long> unlockScript;

    static {
        unlockScript = new DefaultRedisScript<>();
        unlockScript.setLocation(new ClassPathResource("unlock.lua"));
        unlockScript.setResultType(Long.class);
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        String id = ID_PREFIX + Thread.currentThread().getId();
        Boolean lock = redisTemplate.opsForValue().setIfAbsent(key_prefix + name, id, timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(lock);
    }

    @Override
    public void unlock() {
        // 调用lua脚本
        redisTemplate.execute(unlockScript, Collections.singletonList(key_prefix + name), ID_PREFIX + Thread.currentThread().getId());
//        String id = ID_PREFIX + Thread.currentThread().getId();
//        String redisId = redisTemplate.opsForValue().get(key_prefix + name);
//        if (id.equals(redisId)) {
//            redisTemplate.delete(key_prefix + name);
//        }
    }
}
