import org.junit.Test;
import simhash.SimHash;

import java.math.BigInteger;
import java.text.MessageFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by edwardlol on 2017/4/20.
 */
public final class UtilTests {
    //~ Static fields/initializers ---------------------------------------------
    private static Pattern pattern;

    static {
        pattern = Pattern.compile("((?<=\\{)([a-zA-Z_]+)(?=}))");
    }

    //~ Methods ----------------------------------------------------------------

    public String replace(String text, Object... args) {
        return MessageFormat.format(text, args);
    }


    @Test
    public void replaceTest() {
        String text = "hello {0}, welcome to {1}!";
        String user = "Lucy";
        String place = "China";

        String ans = replace(text, user, place);
        System.out.println(ans); // 输出   hello Lucy, welcome to China!
    }

    public String replaceV2(String text, Map<String, Object> map) {
        List<String> keys = new ArrayList<>();

        // 把文本中的所有需要替换的变量捞出来, 丢进 keys
        Matcher matcher = pattern.matcher(text);
        while (matcher.find()) {
            String key = matcher.group();
            if (!keys.contains(key)) {
                keys.add(key);
            }
        }

        // 开始替换, 将变量替换成数字, 并从map中将对应的值丢入 params 数组
        Object[] params = new Object[keys.size()];
        for (int i = 0; i < keys.size(); i++) {
            text = text.replaceAll(keys.get(i), i + "");
            params[i] = map.get(keys.get(i));
        }
        return replace(text, params);
    }

    @Test
    public void testReplaceV2() {
        String text = "hello {user}, welcome to {place}! {place} is very beautiful ";

        Map<String, Object> map = new HashMap<>(2);
        map.put("user", "Lucy");
        map.put("place", "China");

        String res = replaceV2(text, map);
        System.out.println(res);  // hello Lucy, welcome to China! China is very beautiful
    }

    @Test
    public void simhashTest() {
        String aaa = "hello Lucy, welcome to China!";
        SimHash simHash1 = SimHash.Builder.of(aaa).build();

        for (int i = 0; i < 64; i++)
            System.out.print(1);
        System.out.println();
        System.out.println(simHash1.getBinHashCode());

    }

    @Test
    public void bench() {
        long start1 = System.currentTimeMillis();
        for (int i = 0; i < 100000000; i++) {
            String a = String.format("%64s", Long.toBinaryString(1L)).replace(' ', '0');
        }
        long end1 = System.currentTimeMillis();

        long start2 = System.currentTimeMillis();
        for (int i = 0; i < 100000000; i++) {
            String b = String.format("%064d", new BigInteger(Long.toBinaryString(1L)));
        }
        long end2 = System.currentTimeMillis();

        System.out.println("time1: " + (end1 - start1) / 1000 + ", time2: " + (end2 - start2) / 1000);
    }

}

// End UtilTests.java
