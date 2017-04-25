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
import org.ansj.domain.Term;
import org.ansj.library.SynonymsLibrary;
import org.ansj.recognition.impl.SynonymsRecgnition;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by edwardlol on 2017/4/20.
 */
public final class SepTests {
    private static final Pattern stoppingPattern = Pattern.compile("^[\\pP+~$`^=|<>～｀＄＾＋＝｜＜＞￥×≥≦☆Ⅲ°≤丨\\s]+$");
    private Map<String, String> synonymsMap = new HashMap<>();

    //~ Methods ----------------------------------------------------------------

    @Test
    public void test1() {
        String str = "欢迎使用ansj_seg,(ansj中文分词)在这里如果你遇到什么问题都可以联系我.我一定尽我所能.帮助大家.ansj_seg更快,更准,更自由!" ;
        System.out.println(NlpAnalysis.parse(str));
    }

    @Test
    public void test2() {
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

    @Test
    public void test3() {
        String sentence = "温度控制在20℃左右,通风良好的条件下保存,并且与产生污染的货物隔离开来,未浸除咖啡碱的未焙炒咖啡|品种阿拉比卡|咖啡豆|品牌埃塞俄比亚西达摩|60千克/包";
        StringBuilder sb = new StringBuilder();
        for (Term term : DicAnalysis.parse(sentence)) {
            String word = term.getName();
            Matcher matcher = stoppingPattern.matcher(word);
            if (!matcher.find()) {
                sb.append(word).append(' ');
            }
        }
        System.out.println(sb.toString());
    }

    public void replaceTest() {

        this.synonymsMap.put("(?:阿拉比加|阿拉毕卡)", "阿拉比卡");
        this.synonymsMap.put("(?:罗布斯特|罗布斯塔|罗巴斯特|罗巴斯塔|罗伯斯特|罗伯斯塔)", "罗布斯塔");
        this.synonymsMap.put("(?:焙炒|培炒|烘炒|烘培)", "烘焙");
        this.synonymsMap.put("寝除", "浸除");

        // 8703
        this.synonymsMap.put("5座", "五座");
        this.synonymsMap.put("7座", "七座");
        this.synonymsMap.put("不是", "非");
        this.synonymsMap.put("(?:4maitc|4mat1c|4mat2c)", "4matic");
        this.synonymsMap.put("(?:ican-am|can-am)", "canam");
        this.synonymsMap.put("cfm0to", "cfmoto");
        this.synonymsMap.put("bmw", "宝马");
        this.synonymsMap.put("benz", "奔驰");
        this.synonymsMap.put("audi", "奥迪");
        this.synonymsMap.put("(?:mercecles|mercede)", "mercedes");
        this.synonymsMap.put("(?:ferraei|ferrair)", "ferrari");
        this.synonymsMap.put("一气", "一汽");
        this.synonymsMap.put("三凌", "三菱");
        this.synonymsMap.put("(?:克来斯勒|克菜斯勒)", "克莱斯勒");
        this.synonymsMap.put("二厢", "两厢");
        this.synonymsMap.put("保费", "保险费");
        this.synonymsMap.put("爱玛仕", "爱马仕");
        this.synonymsMap.put("乌拉斯", "乌阿斯");
        this.synonymsMap.put("(?:全地行|全地型)", "全地形");


    }

    private String replaceSynonyms(String origin) {
        String result = origin;
        for (Map.Entry<String, String> entry : this.synonymsMap.entrySet()) {
            result = result.replaceAll(entry.getKey(), entry.getValue());
        }
        return result;
    }
}

// End SepTests.java
