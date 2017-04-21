package init;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.DicAnalysis;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by edwardlol on 2017/4/20.
 */
public final class SepUtils {
    //~ Static fields/initializers ---------------------------------------------

    private static final Pattern stoppingPattern = Pattern.compile("^[\\pP+~$`^=|<>～｀＄＾＋＝｜＜＞￥×≥≦☆Ⅲ°≤丨\\s]+$");

    //~ Instance fields --------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    private SepUtils() {

    }

    //~ Methods ----------------------------------------------------------------

    static StringBuilder append(String sentence) {
        StringBuilder sb = new StringBuilder();

        for (Term term : DicAnalysis.parse(sentence)) {
            String word = term.getName();
            Matcher matcher = stoppingPattern.matcher(word);
            if (!matcher.find()) {
                sb.append(word).append(' ');
            }
        }
        try {
            sb.deleteCharAt(sb.length() - 1);
        } catch (RuntimeException e) {
            System.out.println(sb.toString());
        }
        return sb;
    }
}

// End SepUtils.java
