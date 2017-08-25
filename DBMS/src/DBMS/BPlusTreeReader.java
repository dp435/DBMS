package DBMS;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import DBMS.LeafNode;

/**
 * Class to read from a serialized B+ tree index 
 * @author michaelneborak
 *
 */
public class BPlusTreeReader {
	String fileLocation;
	FileInputStream fin;
	FileChannel fc;
	ByteBuffer pageInMemory;
	int pageNumberInMemory = -1;

	// Header page info
	boolean headerRead = false;
	int rootAddress = -1;
	int numberLeaves = -1;
	int treeOrder = -1;

	// State info
	int keyIndex = -1;
	int rIDListPosition = -1;
	LeafNode currentLeaf;
	int lowerBound;
	int upperBound;
	boolean hasUpper = true;

	/**
	 * Constructor for a BPlusTreeReader
	 * @param fileLocation The file location of the serialized
	 * B+ tree index.
	 */
	public BPlusTreeReader(String fileLocation) {
		this.fileLocation = fileLocation;
		try {
			fin = new FileInputStream(fileLocation);
			fc = fin.getChannel();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Read the given page number into memory.
	 * @param pageNumber The page number to be read.
	 */
	public void readPageIntoMemory(int pageNumber) {
		pageInMemory = ByteBuffer.allocate(TupleReaderBinary.BUFFER_SIZE);
		try {
			if (fc.read(pageInMemory, pageNumber * TupleReaderBinary.BUFFER_SIZE) != -1) {
				pageInMemory.rewind();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.pageNumberInMemory = pageNumber;
	}

	/**
	 * Returns if the current in-memory page is a leaf or index page.
	 * 
	 * @return 1 if index page; 0 if leaf page
	 */
	public int indexOrLeaf() {
		return pageInMemory.getInt(BPlusTreeWriter.INDEX_NODE_FLAG);
	}

	/**
	 * Fills header info from the header page in-memory.
	 */
	public void deserializeHeaderInMemory() {
		assert (pageNumberInMemory == 0);
		rootAddress = pageInMemory.getInt(BPlusTreeWriter.ROOT_ADDRESS_INDEX);
		numberLeaves = pageInMemory.getInt(BPlusTreeWriter.LEAF_COUNT_INDEX);
		treeOrder = pageInMemory.getInt(BPlusTreeWriter.TREE_ORDER_INDEX);
		headerRead = true;
	}

	/**
	 * Returns leaf node data from the leaf node page in-memory.
	 * @return The deserialized leaf node.
	 */
	public LeafNode deserializeLeafInMemory() {
		assert (headerRead);
		assert (pageNumberInMemory > 0 && pageNumberInMemory <= numberLeaves);
		assert (indexOrLeaf() == 0);
		LeafNode ln = new LeafNode(pageNumberInMemory);
		int numberOfKeys = pageInMemory.getInt(BPlusTreeWriter.NUMBER_OF_ENTRIES_INDEX);
		int dataPosition = BPlusTreeWriter.ENTRIES_START_INDEX;
		for (int i = 0; i < numberOfKeys; i++) {
			ArrayList<RID> keyRIDs = new ArrayList<>();
			int key = pageInMemory.getInt(dataPosition);
			dataPosition += 4;
			int numRIDs = pageInMemory.getInt(dataPosition);
			dataPosition += 4;
			for (int ridIterator = 0; ridIterator < numRIDs; ridIterator++) {
				int pageID = pageInMemory.getInt(dataPosition);
				dataPosition += 4;
				int tupleID = pageInMemory.getInt(dataPosition);
				dataPosition += 4;
				keyRIDs.add(new RID(pageID, tupleID));
			}
			ln.keys.add(key);
			ln.pointers.add(keyRIDs);
		}

		return ln;
	}

	/**
	 * Returns index node data from the leaf node page in-memory.
	 * @return The deserialized index node.
	 */
	public IndexNode deserializeIndexInMemory() {
		assert (headerRead);
		assert (pageNumberInMemory > numberLeaves && pageNumberInMemory <= rootAddress);
		assert (indexOrLeaf() == 1);
		IndexNode in = new IndexNode(pageNumberInMemory);
		int numberOfKeys = pageInMemory.getInt(BPlusTreeWriter.NUMBER_OF_KEYS_INDEX);
		int dataPosition = BPlusTreeWriter.KEY_START_INDEX;
		for (int i = 0; i < numberOfKeys; i++) {
			in.keys.add(pageInMemory.getInt(dataPosition));
			dataPosition += 4;
		}
		for (int i = 0; i < numberOfKeys + 1; i++) {
			in.children.add(new Node(null, false, pageInMemory.getInt(dataPosition)));
			dataPosition += 4;
		}

		return in;
	}

	/**
	 * Returns node data from the leaf node page in-memory.
	 * @return The deserialized node.
	 */
	public Node deserializeNodeInMemory() {
		assert (headerRead);
		assert (pageNumberInMemory > 0 && pageNumberInMemory <= rootAddress);
		if (indexOrLeaf() == 0) {
			return deserializeLeafInMemory();
		} else {
			return deserializeIndexInMemory();
		}
	}

	/**
	 * Walks the tree from the root down to the searchKey, find the smallest
	 * LeafNode with key k such that the searchKey >= k. 
	 * @return The deserialized leaf node.
	 */
	public LeafNode getLeafFromKey(int searchKey) {
		if (!headerRead) {
			readPageIntoMemory(0);
			deserializeHeaderInMemory();
		}
		readPageIntoMemory(rootAddress);
		IndexNode rootNode = deserializeIndexInMemory();
		return walkSerializedTree(rootNode, searchKey);
	}

	/**
	 * Helper function to walk the tree, looking
	 * for the specified searchKey.
	 * @param node The node to start looking from.
	 * @param searchKey The search key to guide the search.
	 * @return The deserialized leaf node.
	 */
	public LeafNode walkSerializedTree(Node node, int searchKey) {
		if (node.isLeaf) {
			return (LeafNode) node;
		} else {
			IndexNode in = (IndexNode) node;
			if (searchKey < node.keys.get(0)) {
				readPageIntoMemory(in.children.get(0).position);
				return walkSerializedTree(deserializeNodeInMemory(), searchKey);
			} else if (searchKey > node.keys.get(node.keys.size() - 1)) {
				readPageIntoMemory(in.children.get(in.children.size() - 1).position);
				return walkSerializedTree(deserializeNodeInMemory(), searchKey);
			} else {
				for (int i = 0; i < in.keys.size() - 1; i++) {
					if (searchKey >= in.keys.get(i) && searchKey < in.keys.get(i + 1)) {
						readPageIntoMemory(in.children.get(i + 1).position);
						return walkSerializedTree(deserializeNodeInMemory(), searchKey);
					}
				}
				// return null;
			}
		}
		return null;
	}

	/**
	 * Set the lowest key the reader should start at.
	 * @param bound The key to start at.
	 */
	public void setLowerBound(int bound) {
		currentLeaf = getLeafFromKey(bound);
		lowerBound = bound;
		for (int i = 0; i < currentLeaf.keys.size(); i++) {
			if (currentLeaf.keys.get(i) >= lowerBound) {
				keyIndex = i;
				rIDListPosition = 0;
				break;
			}
		}
	}

	/**
	 * Sets the reader to start reading from
	 * the first key in the index.
	 */
	public void setNoLowerBound() {
		readPageIntoMemory(0);
		deserializeHeaderInMemory();
		readPageIntoMemory(1);
		currentLeaf = deserializeLeafInMemory();
		keyIndex = 0;
		rIDListPosition = 0;
	}

	/**
	 * Set the reader to stop reading 
	 * after it reaches the specified bound (key).
	 * @param bound
	 */
	public void setUpperBound(int bound) {
		hasUpper = true;
		upperBound = bound;
	}

	/**
	 * Set the reader to read to the end 
	 * of the index.
	 */
	public void setNoUpperBound() {
		hasUpper = false;
	}

	/**
	 * Get the next RID from the index.
	 * @return the RID
	 */
	public RID getNextRID() {
		assert (currentLeaf != null);
		assert(headerRead);
		while (true) {
			// Are we beyond last key in node?
			if (keyIndex < currentLeaf.keys.size()) {
				// Are we above the upperBound?
				if (keyIndex < 0 || currentLeaf.keys.get(keyIndex) > upperBound && hasUpper) {
					return null;
				}
				if (rIDListPosition < currentLeaf.pointers.get(keyIndex).size()) {
					rIDListPosition++;
					return currentLeaf.pointers.get(keyIndex).get(rIDListPosition - 1);
				} else {
					rIDListPosition = 0;
					keyIndex++;
					continue;
				}
			}
			// If so, get next page, set keyIndex/rIDListPosition = 0, reset
			else {
				if (!getNextPage()) {
					return null;
				} else {
					currentLeaf = deserializeLeafInMemory();
					keyIndex = 0;
				}

			}
		}
	}

	/**
	 * Read the next page into memory.
	 * @return True if there was another page to read.
	 */
	public boolean getNextPage() {
		if (pageNumberInMemory < numberLeaves) {
			readPageIntoMemory(pageNumberInMemory + 1);
			return true;
		} else {
			return false;
		}
	}

	public static void main(String[] arg) {
		String fileloc = "./indexinput/db/indexes/E";
		BPlusTreeReader bptr = new BPlusTreeReader(fileloc);
		bptr.setLowerBound(9997);
		bptr.setNoUpperBound();

		bptr.getNextRID();
		bptr.getNextRID();

		bptr.getNextRID();
		RID test = bptr.getNextRID();
		String tableName = "Boats";
		String inputDirectory = "./indexinput/";
		TupleReaderBinary trb = new TupleReaderBinary(inputDirectory + "/db/data/" + tableName);
		trb.tupleLength = 3;
		trb.SetPageTuplePointer(test.pageID, test.tupleID);
		System.out.println(trb.readFOTuple());
	}
	
	public Integer getNumLeaves() {
		return numberLeaves;
	}

	/**
	 * Reset the reader.
	 */
	public void reset() {
		currentLeaf = null;
	}

}
