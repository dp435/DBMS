package DBMS;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import DBMS.IndexNode;
import DBMS.LeafNode;
import DBMS.Node;

/**
 * Class to write a B+ in memory to disk.
 * @author michaelneborak
 *
 */
public class BPlusTreeWriter {
	String fileLocation;
	BPlusTree tree;
	ByteBuffer pageInMemory;
	FileOutputStream fin;
	FileChannel fc;

	// Header page constants
	public static final int ROOT_ADDRESS_INDEX = 0;
	public static final int LEAF_COUNT_INDEX = 4;
	public static final int TREE_ORDER_INDEX = 8;

	// Generic node page constants
	public static final int INDEX_NODE_FLAG = 0;

	// Index node page constants
	public static final int NUMBER_OF_KEYS_INDEX = 4;
	public static final int KEY_START_INDEX = 8;

	// Leaf node page constants
	public static final int NUMBER_OF_ENTRIES_INDEX = 4;
	public static final int ENTRIES_START_INDEX = 8;

	/**
	 * Constructor for BPlusTreeWriter
	 * @param tree The BPlusTree to write to disk
	 * @param fileLocation The filename to write the tree to
	 */
	public BPlusTreeWriter(BPlusTree tree, String fileLocation) {
		this.fileLocation = fileLocation;
		this.tree = tree;
		try {
			fin = new FileOutputStream(fileLocation);
			fc = fin.getChannel();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Write page in in-memory buffer to disk.
	 */
	public void flush() {
		try {
			if (pageInMemory != null) {
				fc.write(pageInMemory);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Write the header page of the serialized B+ tree file.
	 */
	public void writeHeader() {
		pageInMemory = ByteBuffer.allocate(TupleReaderBinary.BUFFER_SIZE);
		Node root = tree.treeHierarchy.get(tree.treeHierarchy.size() - 1).get(0);
		pageInMemory.putInt(ROOT_ADDRESS_INDEX, root.position);
		pageInMemory.putInt(LEAF_COUNT_INDEX, tree.treeHierarchy.get(0).size());
		pageInMemory.putInt(TREE_ORDER_INDEX, tree.getOrder());
		flush();
	}

	/**
	 * Write an index page of the serialized B+ tree file.
	 */
	public void writeIndexNodePage(IndexNode indexNode) {
		pageInMemory = ByteBuffer.allocate(TupleReaderBinary.BUFFER_SIZE);
		pageInMemory.putInt(INDEX_NODE_FLAG, 1);
		pageInMemory.putInt(NUMBER_OF_KEYS_INDEX, indexNode.keys.size());
		int keyCounter = KEY_START_INDEX;
		for (int i = 0; i < indexNode.keys.size(); i++) {
			pageInMemory.putInt(keyCounter, indexNode.keys.get(i));
			keyCounter += 4;
		}
		for (int i = 0; i < indexNode.children.size(); i++) {
			pageInMemory.putInt(keyCounter, indexNode.children.get(i).position);
			keyCounter += 4;
		}
		flush();
	}

	/**
	 * Write a leaf page of the serialized B+ tree file.
	 */
	public void writeLeafNodePage(LeafNode leafNode) {
		pageInMemory = ByteBuffer.allocate(TupleReaderBinary.BUFFER_SIZE);
		pageInMemory.putInt(INDEX_NODE_FLAG, 0);
		pageInMemory.putInt(NUMBER_OF_ENTRIES_INDEX, leafNode.pointers.size());
		int keyCounter = ENTRIES_START_INDEX;
		for (int i = 0; i < leafNode.keys.size(); i++) {
			ArrayList<RID> ridList = leafNode.pointers.get(i);
			int key = leafNode.keys.get(i);
			pageInMemory.putInt(keyCounter, key);
			keyCounter += 4;
			pageInMemory.putInt(keyCounter, ridList.size());
			keyCounter += 4;
			for (int j = 0; j < ridList.size(); j++) {
				pageInMemory.putInt(keyCounter, ridList.get(j).pageID);
				keyCounter += 4;
				pageInMemory.putInt(keyCounter, ridList.get(j).tupleID);
				keyCounter += 4;
			}
		}
		flush();
	}

	/**
	 * Write the whole tree to disk.
	 */
	public void writeTree() {
		writeHeader();
		for (int i = 0; i < tree.treeHierarchy.get(0).size(); i++) {
			writeLeafNodePage((LeafNode) tree.treeHierarchy.get(0).get(i));
		}
		for (int j = 1; j < tree.treeHierarchy.size(); j++) {
			for (int i = 0; i < tree.treeHierarchy.get(j).size(); i++) {
				writeIndexNodePage((IndexNode) tree.treeHierarchy.get(j).get(i));
			}
		}
	}
}
