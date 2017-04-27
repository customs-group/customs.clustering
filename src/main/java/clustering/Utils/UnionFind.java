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
package clustering.Utils;

/**
 * Data structure to find relationship between sets
 * and methods to union them.
 *
 * @author edwardlol
 *         Created by edwardlol on 17-4-26.
 */
public class UnionFind {
    //~ Instance fields --------------------------------------------------------

    /**
     * Access to set id.
     */
    private int[] id;

    /**
     * Rank of every set.
     * Aka the "depth" of every "tree".
     */
    private int[] rank;

    /**
     * Number of sets.
     */
    private int count;

    //~ Constructors -----------------------------------------------------------

    public UnionFind(int N) {
        // Initialize component id array.
        this.count = N;
        this.id = new int[N];
        for (int i = 0; i < N; i++)
            this.id[i] = i;
        this.rank = new int[N];
        for (int i = 0; i < N; i++)
            this.rank[i] = 1;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Check whether the two points belong to the same set.
     */
    public boolean connected(int p, int q) {
        return find(p) == find(q);
    }

    /**
     * Find the root id of input point p.
     */
    public int find(int p) {
        while (p != this.id[p]) {
            // path compression
            this.id[p] = this.id[this.id[p]];
            p = this.id[p];
        }
        return p;
    }

    public boolean union(int p, int q) {
        int i = find(p);
        int j = find(q);
        if (i == j) {
            return false;
        }

        // 将小树作为大树的子树
        if (this.rank[i] < this.rank[j]) {
            this.id[i] = j;
        } else {
            this.id[j] = i;
            if (this.rank[i] == this.rank[j]) {
                this.rank[j]++;
            }
        }
        this.count--;
        return true;
    }

    public void printRoots() {
        for (int id : this.id) {
            System.out.println(find(id));
        }
    }

    public int[] getId() {
        return id;
    }

    public int getCount() {
        return this.count;
    }
}

// End UnionFind.java
