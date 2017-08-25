package DBMS;

import java.util.List;

import DBMS.IndexNode;

/**
 * This class is the base class for representing a generic node in a B+ tree.
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 *
 */
public class Node {
	boolean isLeaf;
	List<Integer> keys;
	int position;

	/**
	 * Constructor for a node.
	 * @param keys The keys for this node.
	 * @param isLeaf Whether this node is a leaf node,
	 * @param position The position of this node in the pages of the serialized 
	 * index.
	 */
	public Node(List<Integer> keys, boolean isLeaf, int position) {
		this.keys = keys;
		this.isLeaf = isLeaf;
		this.position = position;
	}

	/**
	 * Returns whether this node is underfull.
	 * @param D The D parameter of how full a node can be.
	 * @return Whether this node has less than D keys.
	 */
	public boolean underFull(int D) {
		if (keys.size() < D) {
			return true;
		}
		return false;
	}

	/**
	 * Returns the serialized position of this node.
	 * @return The serialized position of this node.
	 */
	public int getPosition() {
		return position;
	}

	/**
	 * Returns the smallest key of this node.
	 * @return The first (smallest) key of this node.
	 */
	public int getSmallestKey() {
		if (this.isLeaf) {
			return keys.get(0);
		} else {
			assert (this instanceof IndexNode);
			IndexNode thisNode = (IndexNode) this;
			return thisNode.children.get(0).getSmallestKey();
		}
	}
}
