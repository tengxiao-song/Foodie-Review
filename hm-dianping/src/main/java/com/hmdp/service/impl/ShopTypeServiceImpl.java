package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result getlist() {
        String shopTypeStr = stringRedisTemplate.opsForValue().get(RedisConstants.SHOP_TYPE_KEY);
        if (shopTypeStr != null) {
            List<ShopType> list = JSONUtil.toList(shopTypeStr, ShopType.class);
            return Result.ok(list);
        }
        List<ShopType> typeList = query().orderByAsc("sort").list();
        stringRedisTemplate.opsForValue().set(RedisConstants.SHOP_TYPE_KEY, JSONUtil.toJsonStr(typeList));
        return Result.ok(typeList);
    }
}
