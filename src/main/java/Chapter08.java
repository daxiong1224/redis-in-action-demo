import com.sun.xml.internal.ws.api.model.MEP;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

public class Chapter08 {
    private static int HOME_TIMELINE_SIZE = 1000;
    private static int POSTS_PER_PASS = 10;//每次发送消息数量的最大值
    private static int REFILL_USERS_STEP = 50;

    public static final void main(String[] args)
            throws InterruptedException {
        new Chapter08().run();
    }

    public void run()
            throws InterruptedException {
        Jedis conn = new Jedis("192.168.18.129");
        conn.select(5);

        //用来循环生成多个关注者测试数据用
//        for (int i=20;i <30;i++){
//            createUser(conn, "TestUser"+i, "Test User"+i);
//            followUser(conn, i, 2);
//        }


//        conn.flushDB();//清除键

//        testCreateUserAndStatus(conn);
//        conn.flushDB();

//        testFollowUnfollowUser(conn);
//        conn.flushDB();

        testSyndicateStatus(conn);
//        conn.flushDB();
    }

    private void testSyndicateStatus(Jedis conn) {
        System.out.println("---------状态消息的发布与删除------------");

        System.out.println("用户Test User的id：" + createUser(conn, "TestUser", "Test User"));
        System.out.println("用户Test User2的id：" + createUser(conn, "TestUser2", "Test User2"));

        followUser(conn, 1, 2);
        assert conn.zcard("followers:2") == 1;
        assert "1".equals(conn.hget("user:1", "following"));
        if (postStatus(conn, 2, "this is some message content") == 1) {
            System.out.println("消息发布完成!");
        } else {
            System.out.println("消息发布失败");
        }

    }

    private long postStatus(Jedis conn, int uid, String message) {
        return postStatus(conn, uid, message, null);
    }

    private long postStatus(Jedis conn, int uid, String message, Map<String, String> data) {
        long id = createStatus(conn, uid, message, data);
        if (id == -1) {
            return -1;
        }

        String postedString = conn.hget("status:" + id, "posted");//获取发布时间
        if (postedString == null) {
            return -1;
        }

        long posted = Long.parseLong(postedString);
        conn.zadd("profile:" + uid, posted, String.valueOf(id));//将状态消息添加到个人时间线上

        syndicateStatus(conn, uid, id, posted, 0);//将状态推送给关注者
        return id;
    }

    /**
     * 对关注者主页进行更新
     *
     * @param conn
     * @param uid
     * @param postId
     * @param posteTime
     * @param start
     */
    public void syndicateStatus(Jedis conn, long uid, long postId, long posteTime, double start) {
        Set<Tuple> followers = conn.zrangeByScoreWithScores(
                "followers:" + uid,
                String.valueOf(start), "inf",
                0, POSTS_PER_PASS);//以上次更新最后一个关注者为起点，获取接下来1000个关注者

        Transaction trans = conn.multi();
        for (Tuple tuple : followers) {
            String follower = tuple.getElement();
            start = tuple.getScore();
            trans.zadd("home:" + follower, posteTime, String.valueOf(postId));
            trans.zrange("home:" + follower, 0, -1);
            trans.zremrangeByRank("home:" + follower, 0, 0 - HOME_TIMELINE_SIZE - 1);
        }
        trans.exec();

        //关注者超过POSTS_PER_PASS人，在延迟任务继续执行剩余操作
        if (followers.size() >= POSTS_PER_PASS) {
            try {
                Method method = getClass().getDeclaredMethod(
                        "syndicateStatus", Jedis.class, Long.TYPE, Long.TYPE, Long.TYPE, Double.TYPE);
                executeLate("default", method, uid, postId, posteTime, start);
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
        }

    }

    private void executeLate(String queue, Method method, Object... args) {
        MethodThread thread = new MethodThread(this, method, args);
        thread.start();
    }

    public class MethodThread extends Thread {
        private Object instance;
        private Method method;
        private Object[] args;

        public MethodThread(Object instance, Method method, Object... args) {
            this.instance = instance;
            this.method = method;
            this.args = args;
        }

        @Override
        public void run() {
            Jedis conn = new Jedis("192.168.18.129");
            conn.select(5);

            Object[] args = new Object[this.args.length + 1];
            System.arraycopy(this.args, 0, args, 1, this.args.length);
            args[0] = conn;

            try {
                method.invoke(instance, args);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }

    private void testFollowUnfollowUser(Jedis conn) {
        System.out.println("----------测试关注与取消关注用户--------");

        createUser(conn, "TestUser", "Test User");
        createUser(conn, "TestUser2", "Test User2");

        followUser(conn, 1, 2);
        assert conn.zcard("followers:2") == 1;
        assert conn.zcard("followers:1") == 0;
        assert conn.zcard("following:1") == 1;
        assert conn.zcard("following:2") == 0;
        assert "1".equals(conn.hget("user:1", "following"));
        assert "0".equals(conn.hget("user:2", "following"));
        assert "0".equals(conn.hget("user:1", "followers"));
        assert "1".equals(conn.hget("user:2", "followers"));
        System.out.println("关注用户操作成功！");

        assert !unfollowUser(conn, 2, 1);
        assert unfollowUser(conn, 1, 2);
        assert conn.zcard("followers:2") == 0;
        assert conn.zcard("followers:1") == 0;
        assert conn.zcard("following:1") == 0;
        assert conn.zcard("following:2") == 0;
        assert "0".equals(conn.hget("user:1", "following"));
        assert "0".equals(conn.hget("user:2", "following"));
        assert "0".equals(conn.hget("user:1", "followers"));
        assert "0".equals(conn.hget("user:2", "followers"));
        System.out.println("取消关注用户完成！");

    }

    /**
     * 取消关注
     *
     * @param conn
     * @param uid
     * @param otherUid
     * @return
     */
    private boolean unfollowUser(Jedis conn, int uid, int otherUid) {
        String fkey1 = "following:" + uid;
        String fkey2 = "followers:" + otherUid;

        if (conn.zscore(fkey1, String.valueOf(otherUid)) == null) {
            return false;
        }

        Transaction trans = conn.multi();
        trans.zrem(fkey1, String.valueOf(otherUid));
        trans.zrem(fkey2, String.valueOf(uid));
        trans.zcard(fkey1);
        trans.zcard(fkey2);
        trans.zrevrange("profile:" + otherUid, 0, HOME_TIMELINE_SIZE - 1);

        List<Object> response = trans.exec();
        long following = (Long) response.get(response.size() - 3);
        long followers = (Long) response.get(response.size() - 2);
        Set<String> statuses = (Set<String>) response.get(response.size() - 1);

        trans = conn.multi();
        trans.hset("user:" + uid, "following", String.valueOf(following));
        trans.hset("user:" + otherUid, "followers", String.valueOf(followers));
        if (statuses.size() > 0) {
            for (String status : statuses) {
                trans.zrem("home:" + uid, status);
            }
        }

        trans.exec();
        return true;
    }

    /**
     * 关注用户
     *
     * @param conn
     * @param uid      关注者id
     * @param otherUid 被关注者 id
     * @return
     */
    private boolean followUser(Jedis conn, int uid, int otherUid) {
        String fkey1 = "following:" + uid;
        String fkey2 = "followers:" + otherUid;

        if (conn.zscore(fkey1, String.valueOf(otherUid)) != null) {
            return false;
        }

        long now = System.currentTimeMillis();

        //添加到关注则集合和被关注者集合
        Transaction trans = conn.multi();
        trans.zadd(fkey1, now, String.valueOf(otherUid));
        trans.zadd(fkey2, now, String.valueOf(uid));
        trans.zcard(fkey1);
        trans.zcard(fkey2);
        trans.zrevrangeWithScores("profile:" + otherUid, 0, HOME_TIMELINE_SIZE - 1);

        List<Object> response = trans.exec();
        long following = (Long) response.get(response.size() - 3);//取关注的人数
        long followers = (Long) response.get(response.size() - 2);//取粉丝量
        Set<Tuple> statuses = (Set<Tuple>) response.get(response.size() - 1);//取被关注则的HOME_TIMELINE_SIZE条最新状态消息

        trans = conn.multi();
        trans.hset("user:" + uid, "following", String.valueOf(following));//更新关注者数量
        trans.hset("user:" + otherUid, "followers", String.valueOf(followers));//更新粉丝数
        if (statuses.size() > 0) {
            for (Tuple status : statuses) {
                trans.zadd("home:" + uid, status.getScore(), status.getElement());//更新主页时间线
            }
        }
        trans.zremrangeByRank("home:" + uid, 0, 0 - HOME_TIMELINE_SIZE - 1);//保留时间线上1000条状态消息
        trans.exec();
        return true;
    }

    private void testCreateUserAndStatus(Jedis conn) {
        System.out.println("创建用户和状态消息...");

        createUser(conn, "TestUser", "Test User");
        if (createUser(conn, "TestUser", "Test User2") == -1) {
            System.out.println("用户名TestUser已存在，创建失败！");
        }

        if (createStatus(conn, 1, "This is a new status message") == 1) {
            System.out.println("创建消息成功");
        }

    }

    private long createStatus(Jedis conn, long uid, String message) {
        return createStatus(conn, uid, message, null);
    }

    /**
     * 创建状态消息
     *
     * @param conn
     * @param uid
     * @param message
     * @param data
     * @return
     */
    public long createStatus(Jedis conn, long uid, String message, Map<String, String> data) {
        Transaction trans = conn.multi();
        trans.hget("user:" + uid, "login");//获取用户名
        trans.incr("status:id:");//获取新消息的id

        List<Object> response = trans.exec();//一起执行获取
        String login = (String) response.get(0);
        long id = (Long) response.get(1);

        if (login == null) {
            return -1;
        }

        if (data == null) {
            data = new HashMap<String, String>();
        }

        data.put("message", message);
        data.put("posted", String.valueOf(System.currentTimeMillis()));
        data.put("id", String.valueOf(id));
        data.put("uid", String.valueOf(uid));
        data.put("login", login);

        trans = conn.multi();
        trans.hmset("status:" + id, data);
        trans.hincrBy("user:" + uid, "posts", 1);//更新用户已发送消息的量
        trans.exec();
        return id;

    }

    /**
     * 创建用户
     *
     * @param conn
     * @param login 用户名
     * @param name  姓名
     * @return
     */
    private long createUser(Jedis conn, String login, String name) {
        String llogin = login.toLowerCase();
        String lock = acquireLockWithTimeout(conn, "user:" + llogin, 10, 1);

        if (lock == null) {
            return -1;
        }

        if (conn.hget("users:", llogin) != null) {
            return -1;
        }

        long id = conn.incr("user:id");
        Transaction trans = conn.multi();
        trans.hset("users:", llogin, String.valueOf(id));//小写的用户名映射至用户id
        Map<String, String> values = new HashMap<String, String>();
        values.put("login", login);
        values.put("id", String.valueOf(id));
        values.put("name", name);
        values.put("followers", "0");
        values.put("following", "0");
        values.put("posts", "0");
        values.put("signup", String.valueOf(System.currentTimeMillis()));
        trans.hmset("user:" + id, values);
        trans.exec();
        releaseLock(conn, "user:" + llogin, lock);
        return id;
    }


    /**
     * 解锁
     *
     * @param conn
     * @param lockName
     * @param identifier
     * @return
     */
    private boolean releaseLock(Jedis conn, String lockName, String identifier) {
        lockName = "lock:" + lockName;
        while (true) {
            conn.watch(lockName);
            if (identifier.equals(conn.get(lockName))) {
                Transaction trans = conn.multi();
                trans.del(lockName);
                List<Object> result = trans.exec();
                if (result == null) {
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
     * 加锁
     *
     * @param conn
     * @param lockName
     * @param acquireTimeout
     * @param lockTimeout
     * @return
     */
    private String acquireLockWithTimeout(Jedis conn, String lockName, int acquireTimeout, int lockTimeout) {
        String id = UUID.randomUUID().toString();
        lockName = "lock:" + lockName;

        long end = System.currentTimeMillis() + (acquireTimeout * 1000);
        while (System.currentTimeMillis() < end) {
            if (conn.setnx(lockName, id) >= 1) {
                conn.expire(lockName, lockTimeout);
                return id;
            } else if (conn.ttl(lockName) <= 0) {
                conn.expire(lockName, lockTimeout);
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException ie) {
                Thread.interrupted();
            }
        }

        return null;
    }


}
