package someone_else;

import clustering.mst.Driver;
import org.junit.Test;

/**
 * Created by edwardlol on 2017/4/27.
 */
public class MSTDemos {
    //~ Methods ----------------------------------------------------------------

    @Test
    public void mstWorkflowDemo() {
        Driver driver = new Driver();
        String[] args = new String[3];
        args[0] = "/base/sim/isim";
        args[1] = "/base/tf_idf/docCnt";
        args[2] = "/base/mst";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

// End MSTDemos.java
