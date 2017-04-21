package init;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by edwardlol on 2017/4/20.
 */
public class WordSepMapper extends Mapper<LongWritable, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    private Map<Pattern, String> synonymsMap = new HashMap<>();

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.synonymsMap.put(Pattern.compile("阿拉[比毕][加卡]"), "阿拉比卡");
        this.synonymsMap.put(Pattern.compile("罗[布巴伯]斯[特塔]"), "罗布斯塔");
        this.synonymsMap.put(Pattern.compile("[培烘]炒"), "焙炒");
        this.synonymsMap.put(Pattern.compile("烘培"), "焙炒");
        this.synonymsMap.put(Pattern.compile("寝除"), "浸除");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] contents = replaceSynonyms(value.toString()).split("@@");

        String nameAndModel;
        nameAndModel = SepUtils.append(contents[4]) + "##";

        if (contents.length == 6) {
            nameAndModel += SepUtils.append(contents[5]);
        }
        this.outputKey.set(contents[0] + "@@" + contents[1]);
        this.outputValue.set(nameAndModel);
        context.write(this.outputKey, this.outputValue);
    }


    private String replaceSynonyms(String origin) {
        String result = origin;
        for (Map.Entry<Pattern, String> entry : this.synonymsMap.entrySet()) {
            result = entry.getKey().matcher(result).replaceAll(entry.getValue());
        }
        return result;
    }
}

// End WordSepMapper.java
