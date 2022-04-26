package algorithm;

import dataStructure.TreeNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class TreeConvert {

    private ArrayList<String> trees = new ArrayList<>();

    public String getTrees(TreeNode node) {
        tree2string(node);
        return String.join(",", trees);
    }

    private void tree2string(TreeNode node) {
        if (node == null) {
            return;
        }
        int index = node.index;
        int size = node.size;
        int attribute = node.attribute;
        double splitFactor = node.splitFactor;
        String left = "";
        String right = "";
        try {
            left = String.valueOf(node.left.index);
        } catch(Exception e) { }
        try {
            right = String.valueOf(node.right.index);
        } catch(Exception e) { }
        String cache = index+"|"+size+"|"+attribute+"|"+splitFactor+"|"+left+"|"+right;
        trees.add(cache);
        try{
            tree2string(node.left);
        } catch (Exception e) {}
        try {
            tree2string(node.right);
        } catch (Exception e) {}

    }

    public TreeNode string2tree(String trees) {
        ArrayList<TreeNode> vertices = parseString(trees);
        TreeNode iTree = vertices.get(0);
        HashMap<Integer, TreeNode> cache = new HashMap<>();
        for (TreeNode treeNode:vertices) {
            cache.put(treeNode.index, treeNode);
        }
        linkVertex(iTree, 0, cache);
        return iTree;
    }

    private ArrayList<TreeNode> parseString(String trees) {
        ArrayList<TreeNode> vertices = new ArrayList<>();
        String[] treeList = trees.split(",");
        for (String s : treeList) {
            String[] singleTree = s.split("\\|");
            int index = Integer.parseInt(singleTree[0]);
            int size = Integer.parseInt(singleTree[1]);
            int attribute = Integer.parseInt(singleTree[2]);
            double splitFactor = Double.parseDouble(singleTree[3]);
            TreeNode singleVertex = new TreeNode();
            singleVertex.index = index;
            singleVertex.size = size;
            singleVertex.attribute = attribute;
            singleVertex.splitFactor = splitFactor;
            vertices.add(singleVertex);
        }
        return vertices;
    }

    private void linkVertex(TreeNode currentNode, int currentIndex, HashMap<Integer, TreeNode> cache) {

        if (currentNode == null) {
            return;
        }
        try {
            TreeNode left = cache.get(2*currentIndex+1);
            currentNode.left = left;
            linkVertex(left,2*currentIndex+1, cache);
        } catch (Exception e) {
            currentNode.left = null;
        }
        try {
            TreeNode right = cache.get(2*currentIndex+2);
            currentNode.right = right;
            linkVertex(right, 2*currentIndex+2, cache);
        } catch (Exception e) {
            currentNode.right = null;
        }
    }


}
