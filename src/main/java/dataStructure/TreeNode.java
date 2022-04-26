package dataStructure;

public class TreeNode implements Comparable {

    public int index;
    public int attribute;
    public double splitFactor;
    public int size;
    public TreeNode left;
    public TreeNode right;

    public TreeNode(int index, int attribute, double splitFactor, int size) {
        this.index = index;
        this.attribute = attribute;
        this.splitFactor = splitFactor;
        this.size = size;
    }

    public TreeNode() {}


    @Override
    public int compareTo(Object node) {
        int compareIndex = ((TreeNode) node).index;
        return this.index - compareIndex;
    }

}
