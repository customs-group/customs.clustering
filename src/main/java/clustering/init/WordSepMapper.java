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

import clustering.Utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Mapper class to prepare data.
 *
 * @author edwardlol
 *         Created by edwardlol on 2017/4/20.
 */
public class WordSepMapper extends Mapper<LongWritable, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    private Map<String, String> synonymsMap = new HashMap<>();

    private String splitter;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        String extention = FileUtils.getExtension(fileName);

        switch (extention) {
            case "tsv":
                this.splitter = "\t";
                break;
            case "csv":
                this.splitter = ",";
                break;
            default:
                Configuration conf = context.getConfiguration();
                this.splitter = conf.get("column.splitter");
        }

        // TODO: 17-4-21 read from file
        // dicts
        this.synonymsMap.put("公斤", "千克");
        this.synonymsMap.put("不是", "非");
        // 0901
        this.synonymsMap.put("(?:阿拉比加|阿拉毕卡)", "阿拉比卡");
        this.synonymsMap.put("(?:罗布斯特|罗布斯塔|罗巴斯特|罗巴斯塔|罗伯斯特|罗伯斯塔|罗姆斯达)", "罗布斯塔");
        this.synonymsMap.put("(?:焙炒|培炒|烘炒|烘培)", "烘焙");
        this.synonymsMap.put("寝除", "浸除");
        // 8703
        this.synonymsMap.put("5座", "五座");
        this.synonymsMap.put("7座", "七座");
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

    /**
     * Read the input file, extract the commodity info, and split the words
     * in g_name and g_model.
     *
     * @param key   position
     * @param value entry_id@@g_no@@code_ts@@g_name@@[g_model][@@other_columns]
     *              {@inheritDoc}
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] contents = value.toString().split(this.splitter);
        String name = replaceSynonyms(contents[3]);

        String nameAndModel = SepUtils.append(name) + "##";

        if (contents.length > 3) {
            String model = replaceSynonyms(contents[4]);
            nameAndModel += SepUtils.append(model);
        }

        this.outputKey.set(contents[0] + "@@" + contents[1]);
        this.outputValue.set(nameAndModel);
        context.write(this.outputKey, this.outputValue);
    }


    /**
     * Replace all the synonyms in the original sentence.
     */
    // TODO: 17-4-21 if this process takes too long, make it an independent step
    private String replaceSynonyms(String origin) {
        String result = origin;
        for (Map.Entry<String, String> entry : this.synonymsMap.entrySet()) {
            result = result.replaceAll(entry.getKey(), entry.getValue());
        }
        return result;
    }
}

// End WordSepMapper.java
