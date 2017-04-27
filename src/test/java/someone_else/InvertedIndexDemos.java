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

import clustering.inverted_index.Driver;
import org.junit.Test;

/**
 * Use cases of calculating inverted index step.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-25.
 */
public class InvertedIndexDemos {
    //~ Methods ----------------------------------------------------------------

    @Test
    public void defaultDemo() {
        Driver driver = new Driver();
        String[] args = new String[2];
        args[0] = "/base/tf_idf/result";
        args[1] = "/base/inv_index";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void setDeciNum() {
        Driver driver = new Driver();
        String[] args = new String[3];
        args[0] = "/base/tf_idf/result";
        args[1] = "/base/inv_index2";
        args[2] = "2";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Prune the tf_idf lower than the threshold.
    // Normally shouldn't be used, unless you know what you are doing.
    @Test
    public void setPruning() {
        Driver driver = new Driver();
        String[] args = new String[4];
        args[0] = "/base/tf_idf/result";
        args[1] = "/base/inv_index3";
        args[2] = "3";
        args[3] = "0.1";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

// End InvertedIndexDemos.java
