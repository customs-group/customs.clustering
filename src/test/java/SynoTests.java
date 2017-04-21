import org.ansj.domain.Term;
import org.ansj.library.SynonymsLibrary;
import org.ansj.recognition.impl.SynonymsRecgnition;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.junit.Test;

/**
 * Created by edwardlol on 2017/4/20.
 */
public final class SynoTests {
    //~ Methods ----------------------------------------------------------------

    @Test
    public void test1() {
        String str = "欢迎使用ansj_seg,(ansj中文分词)在这里如果你遇到什么问题都可以联系我.我一定尽我所能.帮助大家.ansj_seg更快,更准,更自由!" ;
        System.out.println(DicAnalysis.parse(str));
    }

    @Test
    public void test() {
        //使用默认的同义词词典
        SynonymsRecgnition synonymsRecgnition = new SynonymsRecgnition();

        String str = "我国中国就是华夏,也是天朝";

        for (Term term : ToAnalysis.parse("我国中国就是华夏")) {
            System.out.println(term.getName() + "\t" + (term.getSynonyms()));
        }

        System.out.println("-------------clustering.init library------------------");

        for (Term term : ToAnalysis.parse(str).recognition(synonymsRecgnition)) {
            System.out.println(term.getName() + "\t" + (term.getSynonyms()));
        }

        System.out.println("---------------insert----------------");
        SynonymsLibrary.insert(SynonymsLibrary.DEFAULT, new String[] { "中国", "我国" });

        for (Term term : ToAnalysis.parse(str).recognition(synonymsRecgnition)) {
            System.out.println(term.getName() + "\t" + (term.getSynonyms()));
        }

        System.out.println("---------------append----------------");
        SynonymsLibrary.append(SynonymsLibrary.DEFAULT, new String[] { "中国", "华夏", "天朝" });

        for (Term term : ToAnalysis.parse(str).recognition(synonymsRecgnition)) {
            System.out.println(term.getName() + "\t" + (term.getSynonyms()));
        }

        System.out.println("---------------remove----------------");
        SynonymsLibrary.remove(SynonymsLibrary.DEFAULT, "我国");

        for (Term term : ToAnalysis.parse(str).recognition(synonymsRecgnition)) {
            System.out.println(term.getName() + "\t" + (term.getSynonyms()));
        }

    }
}

// End SynoTests.java
