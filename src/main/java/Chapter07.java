import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Chapter07 {
    private static final Pattern QUERY_RE = Pattern.compile("[+-]?[a-z']{2,}");
    private static final Pattern WORDS_RE = Pattern.compile("[a-z']{2,}");
    private static final Set<String> STOP_WORDS = new HashSet<String>();

    static {
        for (String word :
                ("able about across after all almost also am among " +
                        "an and any are as at be because been but by can " +
                        "cannot could dear did do does either else ever " +
                        "every for from get got had has have he her hers " +
                        "him his how however if in into is it its just " +
                        "least let like likely may me might most must my " +
                        "neither no nor not of off often on only or other " +
                        "our own rather said say says she should since so " +
                        "some than that the their them then there these " +
                        "they this tis to too twas us wants was we were " +
                        "what when where which while who whom why will " +
                        "with would yet you your").split(" ")) {
            STOP_WORDS.add(word);
        }
    }

    private static String CONTENT =
            "this is some random content, look at how it is indexed.";

    public static final void main(String[] args) {
        new Chapter07().run();
    }

    public void run() {
        Jedis conn = new Jedis("192.168.18.129");
        conn.select(15);
        conn.flushDB();

        testIndexDocument(conn);
        testSetOperations(conn);

    }

    private void testSetOperations(Jedis conn) {
        System.out.println("----- 测试设置操作 -----");
        indexDocument(conn, "test", CONTENT);

        Set<String> test = new HashSet<String>();
        test.add("test");

        //有问题!!!!!!
//        Transaction trans = conn.multi();
//        String id = intersect(trans, 30, "content", "indexed");
//        trans.exec();
//        assert test.equals(conn.smembers("idx:" + id));
    }


    /**
     * 交集计算
     *
     * @param trans
     * @param ttl
     * @param items
     * @return
     */
    private String intersect(Transaction trans, int ttl, String... items) {
        return setCommon(trans, "sinterstore", ttl, items);
    }


    /**
     * 对集合进行交、并、差集计算
     *
     * @param trans
     * @param method 方法名
     * @param ttl    过期时间
     * @param items
     * @return
     */
    private String setCommon(Transaction trans, String method,
                             int ttl, String... items) {
        String[] keys = new String[items.length];
        for (int i = 0; i < items.length; i++) {
            keys[i] = "idx:" + items[i];
        }

        String id = UUID.randomUUID().toString();
        try {
            //反射执行传入的方法
            trans.getClass().getDeclaredMethod(method, String.class, String[].class)
                    .invoke(trans, "idx:" + id, keys);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        trans.expire("idx:" + id, ttl);//设置过期时间，自动删除生成的集合
        return id;//

    }

    private void testIndexDocument(Jedis conn) {
        System.out.println("----------测试索引文件-------------");

        System.out.println("标记内容....");
        Set<String> tokens = tokenize(CONTENT);
        System.out.println("过滤后的token集为：" + Arrays.toString(tokens.toArray()));
        assert tokens.size() > 0;

        System.out.println("开始索引内容..");
        int count = indexDocument(conn, "test", CONTENT);

        //结果验证
        assert count == tokens.size();
        Set<String> test = new HashSet<String>();
        test.add("test");
        for (String t : tokens) {
            Set<String> members = conn.smembers("idx:" + t);
            assert test.equals(members);
        }
    }

    /**
     * 建了文章索引
     *
     * @param conn
     * @param docid
     * @param content
     * @return
     */
    private int indexDocument(Jedis conn, String docid, String content) {
        Set<String> words = tokenize(content);
        Transaction trans = conn.multi();
        for (String word : words) {
            trans.sadd("idx:" + word, docid);
        }
        return trans.exec().size();
    }

    /**
     * 内容过滤
     *
     * @param content
     * @return
     */
    private Set<String> tokenize(String content) {
        Set<String> words = new HashSet<String>();
        Matcher matcher = WORDS_RE.matcher(content);//根据预定义的正则表达式获取内容
        while (matcher.find()) {
            String word = matcher.group().trim();//清除前后空
            if (word.length() > 2 && !STOP_WORDS.contains(word)) {//长度大于2 且 不是非用词
                words.add(word);
            }
        }
        return words;
    }
}
