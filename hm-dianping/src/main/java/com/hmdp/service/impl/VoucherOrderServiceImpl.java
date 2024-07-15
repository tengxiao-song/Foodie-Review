package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

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
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Autowired
    private ISeckillVoucherService seckillVoucherService;

    @Autowired
    private RedisIdWorker redisIdWorker;

    @Autowired
    private RedissonClient redissonClient;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    private IVoucherOrderService proxy;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }


    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";
        @Override
        public void run() {
            while (true) {
                try {
                    // 从消息队列中读取信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2000)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    if (list == null || list.isEmpty()) {
                        continue;
                    }
                    // 解析消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    // ack消息
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    // 从消息队列中读取信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0")) // 从pending list中读取
                    );
                    if (list == null || list.isEmpty()) {
                        break;
                    }
                    // 解析消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    // ack消息
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理消息失败", e);
                }
            }
        }


    }
    private void handleVoucherOrder(VoucherOrder vo) {
        Long userId = vo.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.error("不允许重复下单");
            return;
        }
        try {
            proxy.createOrder(vo);    // 调用动态代理的方法，使事务有效
        } finally {
            lock.unlock();
        }
    }

    @Transactional
    public void createOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherOrder).count();
        if (count > 0) {
            log.error("不允许重复下单");
        }
        // 扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0).update(); // 乐观锁 + 成功率优化
        if (!success) {
            log.error("库存不足");
        }
        save(voucherOrder);
    }

    @Override
    public Result secKillVoucher(Long voucherId) {
        // 调用lua脚本判断一人一单和库存, 发送订单信息到消息队列
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(), userId.toString(), orderId + "");
        int r = result.intValue();
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "重复下单");
        }
        proxy = (IVoucherOrderService) AopContext.currentProxy(); // 获取代理对象
        return Result.ok(orderId);
    }

//    @Override
//    public Result secKillVoucher(Long voucherId) {
//        // 调用lua脚本判断一人一单和库存
//        Long userId = UserHolder.getUser().getId();
//        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(), userId.toString());
//        int r = result.intValue();
//        if (r != 0) {
//            return Result.fail(r == 1 ? "库存不足" : "重复下单");
//        }
//        long orderId = redisIdWorker.nextId("order");
//        // 创建订单放入阻塞队列
//        VoucherOrder voucherOrder = new VoucherOrder();
//        voucherOrder.setId(orderId);
//        voucherOrder.setUserId(UserHolder.getUser().getId());
//        voucherOrder.setVoucherId(voucherId);
//        orderTasks.add(voucherOrder);
//        proxy = (IVoucherOrderService) AopContext.currentProxy(); // 获取代理对象
//        return Result.ok(orderId);
//    }


//    @Override
//    public Result secKillVoucher(Long voucherId) {
//
//        // 查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        // 判断秒杀是否开始结束
//        LocalDateTime begin = voucher.getBeginTime();
//        LocalDateTime end = voucher.getEndTime();
//        LocalDateTime now = LocalDateTime.now();
//        if (now.isBefore(begin) || now.isAfter(end)) {
//            return Result.fail("秒杀未开始或已结束");
//        }
//        // 判断库存
//        Integer stock = voucher.getStock();
//        if (stock <= 0) {
//            return Result.fail("库存不足");
//        }
//        Long userId = UserHolder.getUser().getId();
////        synchronized (userId.toString().intern())  // 一人一单, 悲观锁锁定用户
//        // 使用Redis分布式锁
////        SimpleRedisLock simpleRedisLock = new SimpleRedisLock(stringRedisTemplate, "order:" + userId);
////        boolean isLock = simpleRedisLock.tryLock(5);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        boolean isLock = lock.tryLock();
//        if (!isLock) {
//            return Result.fail("不允许重复下单");
//        }
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createOrder(voucherId);    // 调用动态代理的方法，使事务有效
//        } finally {
//            lock.unlock();
////            simpleRedisLock.unlock();
//        }
//    }
}
