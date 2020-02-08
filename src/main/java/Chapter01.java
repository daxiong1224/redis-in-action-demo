import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;

import java.util.*;

public class Chapter01 {
    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;
    private static final int VOTE_SCORE = 432;
    private static final int ARTICLES_PRE_PAGE = 25;

    public static void main(String[] args) {
        new Chapter01().run();
    }

    public void run() {
        Jedis conn = new Jedis("192.168.18.129");
        conn.select(15);
        String articleId = posrtArticle(conn, "大熊", "redis命令大全", "www.baidu.com");
        System.out.println("发布文章成功ID为：" + articleId);

        articleVote(conn, "大熊2号", "article:" + articleId);
        String votes = conn.hget("article:" + articleId, "votes");
        System.out.println("该文章投票的人数为：" + votes);

        //根据文章得分排序
        System.out.println("==========按文章得分排序 start=========");
        List<Map<String,String>> articles = getArticles(conn, 1);
        printArticles(articles);
        System.out.println("==========按文章得分排序 END=========");
    }

    /**
     * 发布文章
     *
     * @param conn
     * @param user
     * @param title
     * @param link
     * @return
     */
    private String posrtArticle(Jedis conn, String user, String title, String link) {
        String articleId = String.valueOf(conn.incr("article:"));//获取新的文章id

        String voted = "voted:" + articleId;
        conn.sadd(voted, user);
        conn.expire(voted, ONE_WEEK_IN_SECONDS);

        long now = System.currentTimeMillis() / 1000;
        String article = "article:" + articleId;
        HashMap<String, String> articleData = new HashMap<String, String>();
        articleData.put("title", title);
        articleData.put("link", link);
        articleData.put("user", user);
        articleData.put("now", String.valueOf(now));
        articleData.put("votes", "1");
        conn.hmset(article, articleData);

        //将文章分别放入按得分、按时间排序的有序集合中
        conn.zadd("score:", now + VOTE_SCORE, article);
        conn.zadd("time:", now, article);
        return articleId;
    }

    /**
     * 对文章投票
     *
     * @param conn
     * @param user
     * @param article
     */
    public void articleVote(Jedis conn, String user, String article) {
        long cutoff = (System.currentTimeMillis() / 1000) - ONE_WEEK_IN_SECONDS;
        if (conn.zscore("time:", article) < cutoff) {//发布时间为一周外的文章不能投票
            return;
        }

        String articleId = article.substring(article.indexOf(":") + 1);
        if (conn.sadd("voted:" + articleId, user) == 1) {
            conn.zincrby("score:", VOTE_SCORE, article);//对该文章加上得分
            conn.hincrBy(article, "votes", 1);//更新文章投票的人数
        }
    }

    public List<Map<String, String>> getArticles(Jedis conn, int page) {
        return getArticles(conn, page, "score:");
    }

    private List<Map<String, String>> getArticles(Jedis conn, int page, String order) {
        int start = (page - 1) * ARTICLES_PRE_PAGE;
        int end = start + ARTICLES_PRE_PAGE - 1;

        Set<String> ids = conn.zrevrange(order, start, end);//降序排序
        List<Map<String, String>> articles = new ArrayList<Map<String, String>>();
        for (String id : ids) {
            Map<String, String> articleData = conn.hgetAll(id);//根据id获取文章信息
            articleData.put("id", id);
            articles.add(articleData);
        }
        return articles;
    }

    //打印文章信息
    private void printArticles(List<Map<String,String>> articles){
        for (Map<String,String> article : articles){
            System.out.println("  id: " + article.get("id"));
            for (Map.Entry<String,String> entry : article.entrySet()){
                if (entry.getKey().equals("id")){
                    continue;
                }
                System.out.println("    " + entry.getKey() + ": " + entry.getValue());
            }
        }
    }
}
