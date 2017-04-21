import org.junit.Test;
import clustering.simhash.SimHash;

import java.math.BigInteger;
import java.util.*;

/**
 * Created by edwardlol on 2017/4/20.
 */
public final class UtilTests {
    //~ Static fields/initializers ---------------------------------------------

    //~ Methods ----------------------------------------------------------------

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

    @Test
    public void segmentTest() {
        String aaa = "hello Lucy, welcome to China!";
        SimHash simHash1 = SimHash.Builder.of(aaa).build();

        System.out.println(simHash1.getBinHashCode());
        System.out.println(simHash1.getSegment(1));
        System.out.println(simHash1.getSegment(2));
        System.out.println(simHash1.getSegment(3));
        System.out.println(simHash1.getSegment(4));
        System.out.println("-------------------------------------------------------");
        simHash1.setNumSegs(3);
        System.out.println(simHash1.getBinHashCode());
        System.out.println(simHash1.getSegment(1));
        System.out.println(simHash1.getSegment(2));
        System.out.println(simHash1.getSegment(3));
    }

    @Test
    public void getNullTest() {
        Map<Integer, Map<Integer, Integer>> map = new HashMap<>();
        Map<Integer, Integer> _map = map.get(1);
        for (Map.Entry<Integer, Integer> entry : _map.entrySet()) {
            System.out.println(entry.getKey());
            System.out.println(entry.getValue());
        }

    }
}

// End UtilTests.java
