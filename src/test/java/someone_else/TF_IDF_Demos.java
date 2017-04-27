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
package someone_else;

import clustering.tf_idf.*;
import org.junit.Test;

/**
 * Use cases for calculating TF-IDF step.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-25.
 */
public class TF_IDF_Demos {
    //~ Methods ----------------------------------------------------------------

    @Test
    public void defaultDemo() {
        Driver driver = new Driver();
        String[] args = new String[2];
        args[0] = "/base/simhash/result";
        args[1] = "/base/tf_idf";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void setWeight() {
        Driver driver = new Driver();
        String[] args = new String[3];
        args[0] = "/base/simhash/result";
        args[1] = "/base/tf_idf2";
        args[2] = "1.2"; // g_name weight, [0, +inf)
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Following demos are decomposition of tf-idf jobs,
    // and should not be used in formal situation.
    // I put them here in case you want to drill into details and debug them.

    @Test
    public void preStepDemo() {
        DocCntDriver driver = new DocCntDriver();
        String[] args = new String[2];
        args[0] = "/base/simhash/result";
        args[1] = "/base/tf_idf_debug/doc_cnt";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step1Demo() {
        TermCntDriver driver = new TermCntDriver();
        String[] args = new String[2];
        args[0] = "/base/simhash/result";
        args[1] = "/base/tf_idf_debug/step1";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step2Demo() {
        TermFreqDriver driver = new TermFreqDriver();
        String[] args = new String[2];
        args[0] = "/base/tf_idf_debug/step1";
        args[1] = "/base/tf_idf_debug/step2";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step2DemoWithWeight() {
        TermFreqDriver driver = new TermFreqDriver();
        String[] args = new String[3];
        args[0] = "/base/tf_idf_debug/step1";
        args[1] = "/base/tf_idf_debug/step2_2";
        args[2] = "1.2";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step3Demo() {
        TF_IDF_Driver driver = new TF_IDF_Driver();
        String[] args = new String[2];
        args[0] = "/base/tf_idf_debug/step2";
        args[1] = "/base/tf_idf_debug/result";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

// End TF_IDF_Demos.java
