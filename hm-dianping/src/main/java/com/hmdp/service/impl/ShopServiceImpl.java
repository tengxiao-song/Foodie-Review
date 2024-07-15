package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
//        Shop shop = cacheClient.queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        Shop shop = cacheClient.queryWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if (shop == null) {
            return Result.fail("商铺不存在");
        }
        return Result.ok(shop);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        if (shop.getId() == null) {
            return Result.fail("商铺id不能为空");
        }
        updateById(shop);
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + shop.getId());
        return Result.ok(shop.getId());
    }


    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public Shop queryWithLogicalExpire(Long id) {
        // 从redis中查询数据
        String shop = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        // 如果不存在则直接返回
        if (StrUtil.isBlank(shop)) {
            return null;
        }
        RedisData data = JSONUtil.toBean(shop, RedisData.class);
        JSONObject data1 = (JSONObject) data.getData();
        Shop theShop = JSONUtil.toBean(data1, Shop.class);
        LocalDateTime expireTime = data.getExpireTime();
        if (expireTime.isAfter(LocalDateTime.now())) {
            return theShop;
        }
        // 重建缓存
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean lock = tryLock(lockKey);
        if (lock) {
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    this.saveShop2Redis(id, RedisConstants.CACHE_SHOP_TTL);
                } finally {
                    unlock(lockKey);
                }
            });
        }
        // 返回数据
        return theShop;
    }

    public boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    public void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    public Shop queryWithMutex(Long id) {
        // 从redis中查询数据
        String shop = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        // 如果存在则直接返回
        if (StrUtil.isNotBlank(shop)) {
            Shop shopObj = JSONUtil.toBean(shop, Shop.class);
            return shopObj;
        }
        // 命中空值，返回错误, 避免缓存穿透
        if (shop != null) {
            return null;
        }
        Shop shopbyId = null;
        try {
            boolean b = tryLock(RedisConstants.LOCK_SHOP_KEY + id);
            if (!b) {
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            // 如果不存在则查询数据库
            shopbyId = getById(id);
            // 如果不存在，返回错误, 存入空值到redis，避免缓存穿透
            if (shopbyId == null) {
                stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 如果存在，则写入redis
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shopbyId), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            unlock(RedisConstants.LOCK_SHOP_KEY + id);
        }
        // 返回数据
        return shopbyId;
    }

    public Shop queryWithPassThrough(Long id) {
        // 从redis中查询数据
        String shop = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        // 如果存在则直接返回
        if (StrUtil.isNotBlank(shop)) {
            Shop shopObj = JSONUtil.toBean(shop, Shop.class);
            return shopObj;
        }
        // 命中空值，返回错误, 避免缓存穿透
        if (shop != null) {
            return null;
        }
        // 如果不存在则查询数据库
        Shop shopbyId = getById(id);
        // 如果不存在，返回错误, 存入空值到redis，避免缓存穿透
        if (shopbyId == null) {
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 如果存在，则写入redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shopbyId), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        // 返回数据
        return shopbyId;
    }

    public void saveShop2Redis(Long id, Long seconds) {
        Shop shop = getById(id);
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(seconds));
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }


    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 判断是否需要根据坐标查询
        if (x == null && y == null) {
            Page<Shop> page = query().eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }
        // 计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        // 按照距离排序，分页查询
        String key = RedisConstants.SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(
                key,
                GeoReference.fromCoordinate(x, y),
                new Distance(5000),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end));
        // 解析出id列表
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(r -> {
                    String id = r.getContent().getName();
                    ids.add(Long.valueOf(id));
                    Distance distance = r.getDistance();
                    distanceMap.put(id, distance);
        });
        // 查询商铺列表
        List<Shop> shops = query().in("id", ids).last("order by field(id, " + StrUtil.join(",", ids) + ")").list();
        for (Shop shop : shops) {
            Distance distance = distanceMap.get(shop.getId().toString());
            shop.setDistance(distance.getValue());
        }
        return Result.ok(shops);
    }
}
