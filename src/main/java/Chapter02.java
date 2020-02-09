import com.google.gson.Gson;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

public class Chapter02 {
    public static void main(String[] args) throws InterruptedException {
        new Chapter02().run();
    }

    public void run() throws InterruptedException {
        Jedis conn = new Jedis("192.168.18.129");
        conn.select(14);
        testLoginCookies(conn);
        testShopppingCartCookies(conn);
        testCacheRequest(conn);
        testCacheRows(conn);

        CleanSessionsThread thread = new CleanSessionsThread(1);
        thread.setName("CleanSessionsThread");
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);

        long s = conn.hlen("login:");
        System.out.println("依然存活的会话数为："+ s);
    }




    /**
     * 登录和缓存cookies
     * @param conn
     * @throws InterruptedException
     */
    private void testLoginCookies(Jedis conn) throws InterruptedException {
        String token = UUID.randomUUID().toString();

        updateToken(conn, token, "大熊", "item");
        System.out.println("登录更新了令牌，令牌值为:" + token);

        String r = checkToken(conn, token);
        System.out.println("通过令牌" + token + "查找用户名的值为：" + r);
    }

    /**
     * 购物车
     * @param conn
     */
    private void testShopppingCartCookies(Jedis conn) {
        System.out.println("\n------- 购物车 -----------------");
        String token = UUID.randomUUID().toString();

        System.out.println("更新会话");
        updateToken(conn, token, "小王", "indeX");
        System.out.println("添加到购物车");
        addToCart(conn, token, "itemY", 3);

    }

    /**
     * 网页请求缓存
     * @param conn
     */
    private void testCacheRequest(Jedis conn) {
        System.out.println("\n------- 网页请求缓存 -----------------");
        String token = UUID.randomUUID().toString();

        Callback callback = new Callback() {
            public String call(String request) {
                return "该内容来自请求 " + request;
            }
        };

        updateToken(conn, token, "小李", "itemX");
        String url = "http://test.com/?item=itemX";
        String result = cacheRequest(conn, url, callback);
        System.out.println("得到的响应内容:" + result);

        System.out.println("为了测试，传入错误的回调内容");
        String result2 = cacheRequest(conn, url, null);
        System.out.println("最终得到了相同的结果:" + result2);
    }

    /**
     * 数据行缓存
     * @param conn
     */
    private void testCacheRows(Jedis conn) throws InterruptedException {
        System.out.println("\n------- 数据行缓存 -----------------");
        String token = UUID.randomUUID().toString();
        scheduleRowCache(conn, "itemX", 5);

        System.out.println("启动线程来缓存行数据:");
        CacheRowsThread thread = new CacheRowsThread();
        thread.start();

        Thread.sleep(1000);
        System.out.println("Our cached data looks like:");
        String r = conn.get("inv:itemX");
        System.out.println(r);
        assert r != null;
        System.out.println();

        System.out.println("We'll check again in 5 seconds...");
        Thread.sleep(5000);
        System.out.println("Notice that the data has changed...");
        String r2 = conn.get("inv:itemX");
        System.out.println(r2);
        System.out.println();
        assert r2 != null;
        assert !r.equals(r2);

        System.out.println("Let's force un-caching");
        scheduleRowCache(conn, "itemX", -1);
        Thread.sleep(1000);
        r = conn.get("inv:itemX");
        System.out.println("The cache was cleared? " + (r == null));
        assert r == null;

        thread.quit();
        Thread.sleep(2000);
    }

    private void scheduleRowCache(Jedis conn, String rowId, int delay) {
        conn.zadd("delay:", delay, rowId);//数据行的延迟值
        conn.zadd("schedule:", System.currentTimeMillis() / 1000, rowId);//
    }

    //缓存请求内容
    private String cacheRequest(Jedis conn, String request, Callback callback) {
        if (!canCache(request)){
            return callback != null ? callback.call(request) : null;
        }

        String pageKey = "cache:" + hashRequest(request);
        String content = conn.get(pageKey);

        if (content == null && callback != null){
            content = callback.call(request);
            conn.setex(pageKey, 300, content);//设置过期时间
        }
        return content;
    }

    /**
     * 判断请求是否能缓存，这里默认为可以
     * @param request
     * @return
     */
    private boolean canCache(String request) {
        return true;
    }

    public String hashRequest(String request) {
        return String.valueOf(request.hashCode());//将请求转为hash串，便于后续查找
    }

    private String checkToken(Jedis conn, String token) {
        return conn.hget("login:", token);
    }

    private void updateToken(Jedis conn, String token, String user, String item) {
        long timestamp = System.currentTimeMillis() / 1000;
        conn.hset("login:", token, user);
        conn.zadd("recent:", timestamp, token);//记录令牌最后一次出现的时间

        if (item != null) {
            conn.zadd("viewed:" + token, timestamp, item);//记录用户浏览过的商品
            conn.zremrangeByRank("viewed:" + token, 0, -26);
            conn.zincrby("viewed:", -1, item);
        }
    }

    private void addToCart(Jedis conn, String session, String item, int count){
        if(count <= 0){
            conn.hdel("cart:" + session, item);//移除指定商品
        } else{
            conn.hset("cart:" + session, item, String.valueOf(count));//添加指定商品到购物车
        }
    }

    private interface Callback{
        String call(String request);
    }

    //清除会话线程
    private class CleanSessionsThread extends Thread{
       private Jedis conn;
       private int limit;//存储会话的最大值,超过该值的需要清除
       private boolean quit;

       public CleanSessionsThread(int limit){
           this.conn = new Jedis("192.168.18.129");
           this.conn.select(14);
           this.limit = limit;
       }

       public void quit(){
           quit = true;
       }

       public void run(){
           while (!quit){
               long size = conn.zcard("recent:");
               if (size <= limit){
                   try {
                       sleep(1000);
                   } catch (InterruptedException e) {
                       e.printStackTrace();
                   }
                   continue;
               }

               //获取需要删除的token ID
               long endIndex = Math.min(size - limit, 100);
               Set<String> tokenSet = conn.zrange("recent:", 0, endIndex -1);
               String[] tokens = tokenSet.toArray(new String[tokenSet.size()]);

               //为要被删除的token构建键名
               ArrayList<String> sessionKeys = new ArrayList<String>();
               for (String token : tokens){
                   sessionKeys.add("viewed:" + token);
                   sessionKeys.add("cart:" + token);
               }

               conn.del(sessionKeys.toArray(new String[sessionKeys.size()]));//删除商品浏览记录
               conn.hdel("login:", tokens);//删除登录记录
               conn.zrem("recent:", tokens);//最近登录集中删除这个用户
           }
       }
    }

    //缓存行线程
    private class CacheRowsThread extends Thread{
        private Jedis conn;
        private boolean quit;

        public CacheRowsThread(){
            this.conn = new Jedis("192.168.18.129");
            this.conn.select(14);
        }

        public void quit(){
            quit = true;
        }

        public void run(){
            Gson gson = new Gson();
            while (!quit){
                Set<Tuple> range = conn.zrangeWithScores("schedule:", 0,0);
                Tuple next = range.size() > 0 ? range.iterator().next() : null;
                long now = System.currentTimeMillis() / 1000;
                if(next == null || next.getScore() > now){
                    try {
                        sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                String rowId = next.getElement();
                double delay = conn.zscore("delay:", rowId);
                if(delay <=0 ){
                    conn.zrem("delay:", rowId);
                    conn.zrem("schedule:", rowId);
                    conn.del("inv:" + rowId);
                    continue;
                }

                Inventory row = Inventory.get(rowId);
                conn.zadd("schedule:", now + delay, rowId);
                conn.set("inv:" + rowId, gson.toJson(row));
            }
        }
    }

    public static class Inventory {
        private String id;
        private String data;
        private long time;

        private Inventory (String id) {
            this.id = id;
            this.data = "data to cache...";
            this.time = System.currentTimeMillis() / 1000;
        }

        public static Inventory get(String id) {
            return new Inventory(id);
        }
    }
}
