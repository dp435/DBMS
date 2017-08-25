package DBMS;

import java.util.ArrayList;
import java.util.List;

/**
 * Class that extends a Node as a LeafNode object.
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 *
 */
public class LeafNode extends Node {
	List<ArrayList<RID>> pointers;

	/**
	 * Leaf node constructor.
	 * @param position The serialized position of this node.
	 */
	public LeafNode(int position) {
		super(new ArrayList<Integer>(), true, position);
		pointers = new ArrayList<ArrayList<RID>>();
	}

	/**
	 * Redistributes the keys and pointers so that both this node
	 * and its left sibiling both have at least D keys.
	 * @param leftSibiling The left sibling for this node, found in the parent.
	 */
	public void redistributeKeys(LeafNode leftSibiling) {
		int leftSize = leftSibiling.keys.size();
		int rightSize = keys.size();
		int k = leftSize + rightSize;
		for (int i = leftSize - 1; i >= k / 2; i--) {
			keys.add(0, leftSibiling.keys.get(i));
			pointers.add(0, leftSibiling.pointers.get(i));
			leftSibiling.keys.remove(i);
			leftSibiling.pointers.remove(i);
		}

	}
}
