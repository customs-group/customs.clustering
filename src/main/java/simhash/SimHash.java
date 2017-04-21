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
package simhash;


import net.openhft.hashing.LongHashFunction;

/**
 * Class to generate a simhash signature for a string.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-21.
 */
public class SimHash {
    //~ Instance fields --------------------------------------------------------

    private final String tokens;

    /**
     * Represents the signature in long.
     */
    private final long hashCode;

    /**
     * The bit number of signature.
     */
    private final int hashbits;

    private int numSegs;

    //~ Constructors -----------------------------------------------------------

    /**
     * Private constructor, use builder to get an instance.
     * Create an instance using specific splitter and bit length.
     *
     * @param tokens   input string to calculate
     * @param splitter the splitter to split tokens, default " "
     * @param hashbits the bit number of signature, default 64
     */
    private SimHash(String tokens, String splitter, int hashbits) {
        this.tokens = tokens;
        this.hashbits = hashbits;

        int[] features = new int[this.hashbits];
        String[] terms = this.tokens.split(splitter);

        for (String term : terms) {
            long termHash = LongHashFunction.xx().hashChars(term);

            // add up bit features
            for (int i = 0; i < this.hashbits; i++) {
                long bitmask = 1L << i;
                features[i] += (termHash & bitmask) != 0 ? 1 : -1;
            }
        }
        long fingerprint = 0L;
        for (int i = 0; i < this.hashbits; i++) {
            if (features[i] >= 0) {
                fingerprint = fingerprint | (1L << i);
            }
        }
        this.hashCode = fingerprint;
    }

    //~ Methods ----------------------------------------------------------------

    public String getTokens() {
        return this.tokens;
    }

    public long getHashCode() {
        return this.hashCode;
    }

    public int getHashbits() {
        return this.hashbits;
    }

    public String getBinHashCode() {
        return String.format("%64s", Long.toBinaryString(this.hashCode)).replace(' ', '0');
    }

    public String[] getSegments(int segmentNumber) {
        return getSegments(this.hashCode, segmentNumber);
    }

    private static String[] getSegments(long signature, int segmentNumber) {
        String[] result = new String[segmentNumber];

        // TODO: 2016/12/10 hard code 64 is not good
        String strSig = String.format("%64s", Long.toBinaryString(signature)).replace(' ', '0');
        int segmentLength = 64 / segmentNumber;
        for (int i = 0; i < segmentNumber; i++) {
            String tmp = strSig.substring(i * segmentLength, (i + 1) * segmentLength - 1);
            result[i] = tmp;
        }
        return result;
    }


    //~ Builder ----------------------------------------------------------------

    public static class Builder {
        private final String _tokens;
        private String _splitter = " ";
        private int _hashbits = 64;

        private Builder(String _tokens) {
            this._tokens = _tokens;
        }

        public static Builder of(String _tokens) {
            return new Builder(_tokens);
        }

        public Builder hashbits(int _hashbits) {
            this._hashbits = _hashbits;
            return this;
        }

        public Builder splitter(String _splitter) {
            this._splitter = _splitter;
            return this;
        }

        public SimHash build() {
            return new SimHash(_tokens, _splitter, _hashbits);
        }
    }
}

// End SimHash.java
