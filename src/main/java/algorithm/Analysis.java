package algorithm;

import dataStructure.TreeNode;

import java.util.List;

public class Analysis {

    /**
     * To calculate Path-Length
     * @param: List<Double> - Input Data Instances, TreeNode iTree, Integer current path-length
     * @return Double - Path-Length
     */
    public static double PathLength(List<Double> instance, TreeNode iTree, int currentPathLength) {

        if (iTree.left == null && iTree.right == null) {

            if(iTree.size > 1) {

                return currentPathLength + Calculate.calculate_Cvalue(iTree.size);
            }else {
                return currentPathLength;
            }

        } else {
            int splitattr = iTree.attribute;
            if (instance.get(splitattr) < iTree.splitFactor) {
                return PathLength(instance, iTree.left, currentPathLength + 1);
            } else {

                return PathLength(instance, iTree.right, currentPathLength + 1);
            }
        }
    }
}
