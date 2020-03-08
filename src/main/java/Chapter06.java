import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.UUID;

public class Chapter06 {

    public static void main(String[] args) throws InterruptedException {
        new Chapter06().run();
    }

    public void run() throws InterruptedException {
        Jedis conn = new Jedis("192.168.18.129");
        conn.select(15);

        testDistributedLocking(conn);
    }

    public void testDistributedLocking(Jedis conn) throws InterruptedException {
        System.out.println("---------分布式锁测试---------");
        conn.del("lock:testlock");
        System.out.println("获取初始化锁...");
        assert acquireLockWithTimeout(conn, "testlock", 1000, 1000) != null;
        System.out.println("取到了锁");
        System.out.println("再次尝试获取锁");
        assert acquireLockWithTimeout(conn, "testlock", 10, 1000) == null;
        System.out.println("未获取到");

        System.out.println("正在等待锁超时");
        Thread.sleep(2000);
        System.out.println("再次去获取锁");
        String lockId = acquireLockWithTimeout(conn, "testlock", 1000, 1000);
        assert lockId != null;
        System.out.println("已取到锁");
        System.out.println("释放锁中..");
        assert releaseLock(conn, "testlock", lockId);
        System.out.println("释放锁成功");

        System.out.println("再次获取锁");
        assert acquireLockWithTimeout(conn, "testlock", 1000, 1000) != null;
        System.out.println("取到了锁");
        conn.del("lock:testlock");
    }

    /**
     * 释放锁
     * @param conn
     * @param lockName
     * @param identifier
     * @return
     */
    private boolean releaseLock(Jedis conn, String lockName, String identifier) {
        String lockKey = "lock:" + lockName;

        while (true) {
            conn.watch(lockKey);
            if (identifier.equals(conn.get(lockKey))){
                Transaction trans = conn.multi();
                trans.del(lockKey);
                List<Object> results = trans.exec();
                if (results == null) {
                    continue;
                }
                return true;
            }

            conn.unwatch();
            break;
        }
        return false;
    }

    /**
     * 获取锁
     * @param conn
     * @param lockName
     * @param acquireTimeout 获取锁超时时间
     * @param locktimeout 锁的过期时间
     * @return
     */
    private String acquireLockWithTimeout(Jedis conn, String lockName, int acquireTimeout, int locktimeout) {
        String identifier = UUID.randomUUID().toString();
        String lockKey ="lock:" + lockName;
        int lockExpire = (int)(locktimeout / 1000);

        long end = System.currentTimeMillis() + acquireTimeout;//设置获取超时时间
        while (System.currentTimeMillis() < end){
            if(conn.setnx(lockKey, identifier) == 1){//尝试获取锁
                conn.expire(lockKey, lockExpire);//设置锁过期时间
                return identifier;
            }

            if (conn.ttl(lockKey) == -1 ){
                conn.expire(lockKey, lockExpire);
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }


}
