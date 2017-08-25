package DBMS;

import java.util.ArrayList;
import java.util.List;

/**
 * Class that extends a Node as an Index node object.
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 *
 */
public class IndexNode extends Node {
	List<Node> children;

	/**
	 * Index node constructor
	 * @param position The serialized position of this node.
	 */
	public IndexNode(int position) {
		super(new ArrayList<Integer>(), false, position);
		this.children = new ArrayList<>();
	}

	/**
	 * Redistributes the keys of this node with the left
	 * sibling so that both nodes have more than D keys.
	 * @param leftSibling The left sibling of this node,
	 * found in the parent node.
	 */
	public void redistributeChildren(IndexNode leftSibling) {
		int leftSize = leftSibling.children.size();
		int rightSize = children.size();
		int m = leftSize + rightSize;
		for (int i = leftSize - 1; i >= m / 2; i--) {
			children.add(0, leftSibling.children.get(i));
			leftSibling.children.remove(i);
		}
	}
}
