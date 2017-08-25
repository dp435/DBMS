package DBMS;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Class to implement a B+ Tree.
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 *
 */
public class BPlusTree {
	int D;
	public ArrayList<ArrayList<Node>> treeHierarchy;
	String tableName;
	TupleReaderBinary trb;
	public List<String> ordering;
	int keyPosition = -1;
	int indexPage = 0;

	/**
	 * Constructor for a B+ Tree.
	 * @param tableName The table name that this tree will build.
	 * @param indexKey The field name that this tree will be indexed on.
	 * @param ordering The ordered list of field names corresponding to this table.
	 * @param clustered Whether this index will be clustered.
	 * @param D The order of this tree.
	 */
	public BPlusTree(String tableName, String indexKey, List<String> ordering, boolean clustered, int D) {
		this.D = D;
		this.treeHierarchy = new ArrayList<ArrayList<Node>>();
		this.tableName = tableName;
		String inputDirectory = DatabaseCatalog.getInstance().getInputDirectory();
		trb = new TupleReaderBinary(inputDirectory + "/db/data/" + tableName);
		trb.tupleLength = ordering.size();
		this.ordering = ordering;// DatabaseCatalog.getInstance().GetTable(tableName).getFields();
		for (int i = 0; i < ordering.size(); i++) {
			if (ordering.get(i).equals(indexKey)) {
				keyPosition = i;
				break;
			}
		}

		if (clustered) {
			ArrayList<FieldOrderedTuple> tupleSet = new ArrayList<>();
			FieldOrderedTuple t = new FieldOrderedTuple();
			while ((t = trb.readFOTuple()) != null) {
				tupleSet.add(t);
			}
			tupleSet.sort(new FOTComparator());
			File original = new File(inputDirectory + "/db/data/" + tableName);
			original.delete();
			TupleWriterBinary twb = new TupleWriterBinary(inputDirectory + "/db/data/" + tableName);
			for (int i = 0; i < tupleSet.size(); i++) {
				twb.writeTuple(tupleSet.get(i));
			}
			twb.flush();
			trb = new TupleReaderBinary(inputDirectory + "/db/data/" + tableName);
			trb.tupleLength = ordering.size();
		}
	}

	/**
	 * Sanity-check function to ensure tree is valid after construction.
	 */
	public void checkConstraints() {
		LeafNode ln1 = (LeafNode) treeHierarchy.get(0).get(0);
		LeafNode ln0;
		for (int i = 1; i < ln1.keys.size(); i++) {
			assert (ln1.keys.get(i) > ln1.keys.get(i - 1));
		}
		assert (ln1.keys.size() == ln1.pointers.size());
		for (int i = 1; i < treeHierarchy.get(0).size(); i++) {
			if (treeHierarchy.get(0).get(i).isLeaf) {
				ln1 = (LeafNode) treeHierarchy.get(0).get(i);
				ln0 = (LeafNode) treeHierarchy.get(0).get(i - 1);
				assert (ln1.keys.size() == ln1.pointers.size());
				if (i < treeHierarchy.get(0).size() - 2) {
					assert (ln1.keys.size() == ln0.keys.size());
				}
				assert (ln1.keys.get(0) > ln0.keys.get(ln0.keys.size() - 1));
				for (int k = 1; k < ln1.keys.size(); k++) {
					assert (ln1.keys.get(k) > ln1.keys.get(k - 1));
				}
			}
		}
	}

	/**
	 * Populates leaf nodes; should be called before fillIndexNodes.
	 */
	public void fillLeafNodes() {
		FieldOrderedTuple ret;
		int absoluteKeyPosition = 0;
		List<KeyRIDPair> krPairList = new ArrayList<KeyRIDPair>();
		while ((ret = trb.readFOTuple()) != null) {
			KeyRIDPair krPair = new KeyRIDPair(ret.getField(keyPosition), trb.absoluteIDToRID(absoluteKeyPosition));
			krPairList.add(krPair);
			absoluteKeyPosition++;
		}
		krPairList.sort(new KRComparator());
		List<KeyRIDsPair> compressedKRSList = new ArrayList<>();
		int previousKey = krPairList.get(0).getKey();
		KeyRIDsPair krsPair = new KeyRIDsPair(previousKey);
		for (int i = 0; i < krPairList.size(); i++) {
			if (krPairList.get(i).getKey() == previousKey) {
				krsPair.AddRID(krPairList.get(i).getRID());
			} else {
				compressedKRSList.add(krsPair);
				previousKey = krPairList.get(i).getKey();
				krsPair = new KeyRIDsPair(krPairList.get(i).getKey());
				krsPair.AddRID(krPairList.get(i).getRID());
			}
		}
		compressedKRSList.add(krsPair);
		krPairList.clear();
		indexPage = 1;
		ArrayList<Node> leafNodes = new ArrayList<Node>();
		LeafNode ln = new LeafNode(indexPage);

		for (int i = 0; i < compressedKRSList.size(); i++) {
			if (i % (2 * D) == 0 && i != 0) {
				leafNodes.add(ln);
				indexPage++;
				ln = new LeafNode(indexPage);
				KeyRIDsPair krs = compressedKRSList.get(i);
				ln.keys.add(krs.getKey());
				ln.pointers.add(compressedKRSList.get(i).getRIDs());
			} else {
				ln.keys.add(compressedKRSList.get(i).getKey());
				ln.pointers.add(compressedKRSList.get(i).getRIDs());
			}
		}
		leafNodes.add(ln);
		if (ln.underFull(D) && leafNodes.size() > 1) {
			LeafNode left = (LeafNode) leafNodes.get(leafNodes.size() - 2);
			ln.redistributeKeys(left);
		}
		compressedKRSList.clear();
		treeHierarchy.add(leafNodes);
	}

	/**
	 * 
	 * @param level The number of nodes away from the leaf node.
	 * @return Whether the just processed level was the root node.
	 */
	public boolean fillIndexNodes(int level) {
		boolean isRoot = false;
		assert (treeHierarchy.size() > 0);
		assert (treeHierarchy.get(0).get(0) instanceof LeafNode);
		int nodeIterator = 0;
		Node n;
		ArrayList<Node> indexNodes = new ArrayList<>();
		IndexNode in = null;
		while (nodeIterator < treeHierarchy.get(level).size()) {
			n = treeHierarchy.get(level).get(nodeIterator);
			if (nodeIterator % (2 * D + 1) == 0) {
				// System.out.println("NEXTindexNODE");
				indexPage++;
				if (nodeIterator > 0) {
					indexNodes.add(in);
				}
				in = new IndexNode(indexPage);
				// System.out.println("_________________" + n.keys.get(0) + "
				// at: " + n.position);
				in.children.add(n);
			} else {
				// System.out.println(n.keys.get(0) + "at: " + n.position);
				in.children.add(n);
			}
			nodeIterator++;
		}
		indexNodes.add(in);
		if (indexNodes.size() == 1) {
			isRoot = true;
		} else {
			IndexNode secondToLast = (IndexNode) indexNodes.get(indexNodes.size() - 2);
			if (in.children.size() + secondToLast.children.size() < 3 * D + 2) {
				in.redistributeChildren(secondToLast);
			}
		}
		System.out.println("Done with filling children");
		for (Node node : indexNodes) {
			IndexNode inode = (IndexNode) node;
			for (int keyIterator = 1; keyIterator < inode.children.size(); keyIterator++) {
				node.keys.add(inode.children.get(keyIterator).getSmallestKey());
			}
		}
		System.out.println("Done filling keys");
		treeHierarchy.add(indexNodes);
		return isRoot;
	}

	/**
	 * Helper class that pairs a key with a list of RIDs.
	 * @author Daniel Park (dp435) & Michael Neborak (mln45)
	 *
	 */
	class KeyRIDsPair {
		int key;
		ArrayList<RID> rIDs;

		public KeyRIDsPair(int key) {
			this.key = key;
			this.rIDs = new ArrayList<RID>();
		}

		public KeyRIDsPair(int key, ArrayList<RID> rIDs) {
			this.key = key;
			this.rIDs = rIDs;
		}

		public void AddRID(RID rid) {
			this.rIDs.add(rid);
		}

		public int getKey() {
			return key;
		}

		public ArrayList<RID> getRIDs() {
			return rIDs;
		}
	}

	/**
	 * Helper class that stores a key with a 
	 * corresponding RID.
	 * @author Daniel Park (dp435) & Michael Neborak (mln45)
	 *
	 */
	class KeyRIDPair {
		int key;
		RID rID;

		public KeyRIDPair(int key, RID rID) {
			this.key = key;
			this.rID = rID;
		}

		public int getKey() {
			return key;
		}

		public RID getRID() {
			return rID;
		}

		@Override
		public String toString() {
			return String.valueOf(key) + ":" + rID;
		}

	}

	/**
	 * Class to compare KeyRIDPair objects.
	 * @author Daniel Park (dp435) & Michael Neborak (mln45)
	 *
	 */
	class KRComparator implements Comparator<KeyRIDPair> {
		
		@Override
		public int compare(KeyRIDPair o1, KeyRIDPair o2) {
			if (o1.getKey() > o2.getKey()) {
				return 1;
			} else if (o1.getKey() == o2.getKey()) {
				return 0;
			} else {
				return -1;
			}
		}
	}

	/**
	 * Class to compare FieldOrderedTuple objects.
	 * @author Daniel Park (dp435) & Michael Neborak (mln45)
	 *
	 */
	class FOTComparator implements Comparator<FieldOrderedTuple> {

		@Override
		public int compare(FieldOrderedTuple o1, FieldOrderedTuple o2) {
			// TODO Auto-generated method stub
			if (o1.getField(keyPosition) > o2.getField(keyPosition)) {
				return 1;
			} else if (o1.getField(keyPosition) == o2.getField(keyPosition)) {
				return 0;
			} else {
				return -1;
			}
		}

	}

	/**
	 * Constructs the tree given a valid table file.
	 */
	public void constructTree() {
		fillLeafNodes();
		int level = 0;
		while (!fillIndexNodes(level)) {
			level++;
		}
	}
	
	/**
	 * Function to the return the order of this tree.
	 * @return The order, D, of this tree.
	 */
	public int getOrder(){
		return D;
	}

	public static void main(String[] args) {
	    BPlusTree bp = new BPlusTree("Sailors", "A", Arrays.asList("A", "B", "C"), true, 15);
		//BPlusTree bp = new BPlusTree("Boats", "E", Arrays.asList("D", "E", "F"), false, 10);
		bp.constructTree();
		bp.checkConstraints();

		BPlusTreeWriter bptw = new BPlusTreeWriter(bp, "./indexinput/db/indexes/A");
		bptw.writeTree();

	}
}
