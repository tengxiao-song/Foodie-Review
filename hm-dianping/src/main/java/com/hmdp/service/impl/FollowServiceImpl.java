package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private IUserService userService;

    @Override
    public Result follow(Long id, Boolean isFollow) {
        Long currUserId = UserHolder.getUser().getId();
        String key = "follow:" + currUserId;
        if (isFollow) {
            Follow follow = new Follow();
            follow.setUserId(currUserId);
            follow.setFollowUserId(id);
            boolean isSuccess = save(follow);
            if (isSuccess) {
                redisTemplate.opsForSet().add(key, id.toString());
            }
        } else {
            boolean isSuccess = remove(new QueryWrapper<Follow>().eq("user_id", currUserId).eq("follow_user_id", id));
            if (isSuccess) {
                redisTemplate.opsForSet().remove(key, id.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result isFollow(Long id) {
        Long currUserId = UserHolder.getUser().getId();
        Integer count = query().eq("user_id", currUserId).eq("follow_user_id", id).count();
        return Result.ok(count > 0);
    }

    @Override
    public Result followCommons(Long id) {
        Long currUserId = UserHolder.getUser().getId();
        String key = "follow:" + currUserId;
        String key2 = "follow:" + id;
        Set<String> intersect = redisTemplate.opsForSet().intersect(key, key2);
        if (intersect == null || intersect.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        List<UserDTO> users = userService.listByIds(ids).stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());
        return Result.ok(users);
    }
}
