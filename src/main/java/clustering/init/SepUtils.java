/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package clustering.init;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.DicAnalysis;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Util methods for seperating sentences.
 *
 * @author edwardlol
 *         Created by edwardlol on 2017/4/20.
 */
public final class SepUtils {
    //~ Static fields/initializers ---------------------------------------------

    private static final Pattern stoppingPattern = Pattern.compile("^[\\pP+~$`^=|<>～｀＄＾＋＝｜＜＞￥×≥≦☆Ⅲ°≤丨\\s]+$");

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
            // usually because there's nothing in g_model
            // do nothing here
        }
        return sb;
    }
}

// End SepUtils.java
