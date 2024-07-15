package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.core.toolkit.BeanUtils;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 校验手机号是否合法, 不合法返回错误信息
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式不正确");
        }
        // 生成验证码
        String code = RandomUtil.randomNumbers(6);
        // 将验证码存入redis
        redisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, RedisConstants.LOGIN_CODE_TTL, TimeUnit.MINUTES);
        // 发送验证码 (跳过)
        log.info("向手机号{}发送验证码：{}", phone, code);
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 校验手机号是否合法, 不合法返回错误信息
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式不正确");
        }
        // 校验验证码是否正确
//        Object cacheCode = session.getAttribute("code"); // 能到这一步，说明验证码已经发送过了，session已经创建了其中一定有code，新request会复用这个session
        String code = loginForm.getCode();
        String cacheCode = redisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        if (cacheCode == null || !cacheCode.equals(code)) {
            return Result.fail("验证码错误");
        }
        // 根据手机号查询用户信息
        User user = getOne(Wrappers.<User>lambdaQuery().eq(User::getPhone, phone));
        if (user == null) {
            user = createUserWithPhone(phone);
        }
        // 将用户信息存入redis
        String token = UUID.randomUUID().toString(true);
        UserDTO userDTO = new UserDTO();
        BeanUtil.copyProperties(user, userDTO);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(), CopyOptions.create().setIgnoreNullValue(true).setFieldValueEditor((field, value)-> {
            return value.toString();
        }));
        redisTemplate.opsForHash().putAll(LOGIN_USER_KEY + token, userMap);
        redisTemplate.expire(LOGIN_USER_KEY + token, Duration.ofSeconds(LOGIN_USER_TTL));
        return Result.ok(token);
     }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        save(user);
        return user;
    }

    @Override
    public Result sign() {
        Long id = UserHolder.getUser().getId();
        LocalDateTime now = LocalDateTime.now();
        String key = RedisConstants.USER_SIGN_KEY + id + now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        int dayOfMonth = now.getDayOfMonth();
        // 使用bitmap签到，offset和日期-1对应index
        redisTemplate.opsForValue().setBit(key, dayOfMonth-1, true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        Long id = UserHolder.getUser().getId();
        LocalDateTime now = LocalDateTime.now();
        String key = RedisConstants.USER_SIGN_KEY + id + now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        int dayOfMonth = now.getDayOfMonth();
        // 拿到这个月的签到数据，从第0位开始取dayOfMonth个
        List<Long> result = redisTemplate.opsForValue().bitField(key,
                BitFieldSubCommands.create().get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0));
        // 统计连续签到天数，每一位代表一天，从右往左，遇到0就停止
        if (result == null || result.isEmpty()) {
            return Result.ok(0);
        }
        Long num = result.get(0);
        if (num == null || num == 0) {
            return Result.ok(0);
        }
        int count = 0;
        while (true) {
            if ((num & 1) == 0) {
                break;
            } else {
                count++;
                num >>>= 1;
            }
        }
        return Result.ok(count);
    }
}
