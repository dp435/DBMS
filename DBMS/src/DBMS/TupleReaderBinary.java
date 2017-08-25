package DBMS;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import DBMS.RID;

/**
 * This class reads tuples from a binary file.
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 *
 */
public class TupleReaderBinary {
	String fileLocation;
	int tupleLength = -1;
	int tupleIndex = 0;
	int tuplesPerPage = -1;
	int pagePointer = -1;
	Hashtable<String, String> ht;
	ByteBuffer pageInMemory;

	FieldOrderedTuple currentTuple;
	int partitionStartPage = 0;
	int partitionStartIndex = 0;
	FieldOrderedTuple previousTuple;
	int previousPartitionStartPage = 0;
	int previousPartitionStartIndex = 0;

	FileInputStream fin;
	FileChannel fc;
	boolean closed;
	public static final int BUFFER_SIZE = 4096;
	public static final int METADATA_SIZE = 8;
	public static final int BYTE_SIZE = 4;
	public List<String> keyNames;
	boolean initializedPages;

	private String tableName;
	private String prefixName;

	/**
	 * Constructor for TupleReaderBinary
	 * @param tableName The table name of the table this file refers to
	 * @param aliasName The alias name (if any) of the table the file refers to
	 * @param currentTable The relevant table-meta data for the table this file refers to
	 */
	public TupleReaderBinary(String tableName, String aliasName, TableInfo currentTable) {
		this.fileLocation = currentTable.getLocation();
		this.keyNames = currentTable.getFields();
		this.tableName = tableName;
		initializedPages = false;
		currentTuple = null;
		previousTuple = null;
		if (aliasName != null && !aliasName.equals("")) {
			prefixName = aliasName;
		} else {
			prefixName = tableName;
		}
		try {
			fin = new FileInputStream(fileLocation);
			fc = fin.getChannel();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Constructor for only reading a binary tuple
	 * based on its file location.
	 * @param fileLocation The location of the binary file
	 */
	public TupleReaderBinary(String fileLocation) {
		this.fileLocation = fileLocation;
		initializedPages = false;
		currentTuple = null;
		previousTuple = null;

		try {
			fin = new FileInputStream(fileLocation);
			fc = fin.getChannel();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Set this reader back to its beginning marker.
	 */
	public void reset() {
		try {
			if (!closed) {
				try {
					fin.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			fin = new FileInputStream(fileLocation);
			fc = fin.getChannel();
			pageInMemory = null;
			tupleLength = -1;
			tupleIndex = 0;
			tuplesPerPage = -1;
			pagePointer = -1;
			closed = false;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Read the next tuple from the referenced file.
	 * @param ES True if the reader is External Sort; false otherwise
	 * @return The next tuple from the file.
	 */
	public Tuple readTuple(boolean ES) {
		FieldOrderedTuple temp = new FieldOrderedTuple();
		if (closed) {
			return null;
		}
		try {
			if (tupleIndex < tuplesPerPage && initializedPages) {
				ht = new Hashtable<>();
				List<String> ordering = new ArrayList<>();
				for (int i = 0; i < tupleLength; i++) {
					int calculatedIndex = 8 + (tupleIndex) * tupleLength * 4 + 4 * i;
					if (ES) {
						ht.put(keyNames.get(i), Integer.toString(pageInMemory.getInt(calculatedIndex)));
						ordering.add(keyNames.get(i));
					} else {
						ht.put(prefixName + "." + keyNames.get(i),
								Integer.toString(pageInMemory.getInt(calculatedIndex)));
						ordering.add(prefixName + "." + keyNames.get(i));
					}
					temp.addField(pageInMemory.getInt(calculatedIndex));
				}
				if (!temp.equals(currentTuple)) {
					previousPartitionStartPage = partitionStartPage;
					previousPartitionStartIndex = partitionStartIndex;

					partitionStartPage = pagePointer;
					partitionStartIndex = tupleIndex;
					previousTuple = currentTuple;
					currentTuple = temp;
				}
				tupleIndex++;
				Tuple ret = new Tuple(tableName, ht, ordering);
				return ret;
			} else {
				pageInMemory = ByteBuffer.allocate(BUFFER_SIZE);
				pagePointer++;
				if (fc.read(pageInMemory, pagePointer * BUFFER_SIZE) != -1) {
					pageInMemory.rewind();
					tupleLength = pageInMemory.getInt();
					tupleIndex = initializedPages ? 0 : tupleIndex;
					initializedPages = true;
					tuplesPerPage = pageInMemory.getInt();
					if (tupleLength != keyNames.size()) {
						System.out.println(
								"ERROR TUPLE LENGTH: " + tupleLength + "   KEY NAMES PROVIDED: " + keyNames.size());
					}
					if (tupleIndex < tuplesPerPage) {
						ht = new Hashtable<>();
						List<String> ordering = new ArrayList<>();
						for (int i = 0; i < tupleLength; i++) {
							int calculatedIndex = 8 + (tupleIndex) * tupleLength * 4 + 4 * i;
							if (ES) {
								ht.put(keyNames.get(i), Integer.toString(pageInMemory.getInt(calculatedIndex)));
								temp.addField(pageInMemory.getInt(calculatedIndex));
								ordering.add(keyNames.get(i));
							} else {
								ht.put(prefixName + "." + keyNames.get(i),
										Integer.toString(pageInMemory.getInt(calculatedIndex)));
								temp.addField(pageInMemory.getInt(calculatedIndex));
								ordering.add(prefixName + "." + keyNames.get(i));
							}

						}
						if (!temp.equals(currentTuple)) {
							previousPartitionStartPage = partitionStartPage;
							previousPartitionStartIndex = partitionStartIndex;

							partitionStartPage = pagePointer;
							partitionStartIndex = tupleIndex;
							previousTuple = currentTuple;
							currentTuple = temp;
						}
						tupleIndex++;
						Tuple ret = new Tuple(tableName, ht, ordering);
						return ret;
					} else {
						fin.close();
						closed = true;
						return null;
					}
				} else {
					fin.close();
					closed = true;
					return null;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * This function reads a tuple just like
	 * readTuple(), but returns a field ordered
	 * tuple for conversion purposes.
	 * 
	 * @return The next tuple, converted to a
	 * field ordered tuple.
	 */
	public FieldOrderedTuple readFOTuple() {
		FieldOrderedTuple temp = new FieldOrderedTuple();
		if (closed) {
			return null;
		}
		try {
			FieldOrderedTuple fot = new FieldOrderedTuple();
			if (tupleIndex < tuplesPerPage && initializedPages) {
				ht = new Hashtable<>();
				for (int i = 0; i < tupleLength; i++) {
					int calculatedIndex = 8 + (tupleIndex) * tupleLength * 4 + 4 * i;
					fot.addField(pageInMemory.getInt(calculatedIndex));
					// ht.put(prefixName + "." + keyNames.get(i),
					// Integer.toString(pageInMemory.getInt(calculatedIndex)));
					temp.addField(pageInMemory.getInt(calculatedIndex));
				}
				if (!temp.equals(currentTuple)) {
					previousPartitionStartPage = partitionStartPage;
					previousPartitionStartIndex = partitionStartIndex;

					partitionStartPage = pagePointer;
					partitionStartIndex = tupleIndex;
					previousTuple = currentTuple;
					currentTuple = temp;
				}
				tupleIndex++;
				return fot;
			} else {
				pageInMemory = ByteBuffer.allocate(BUFFER_SIZE);
				pagePointer++;
				if (fc.read(pageInMemory, pagePointer * BUFFER_SIZE) != -1) {
					pageInMemory.rewind();
					tupleLength = pageInMemory.getInt();
					tupleIndex = initializedPages ? 0 : tupleIndex;
					initializedPages = true;
					tuplesPerPage = pageInMemory.getInt();

					if (tupleIndex < tuplesPerPage) {
						ht = new Hashtable<>();
						for (int i = 0; i < tupleLength; i++) {
							int calculatedIndex = 8 + (tupleIndex) * tupleLength * 4 + 4 * i;
							fot.addField(pageInMemory.getInt(calculatedIndex));
							temp.addField(pageInMemory.getInt(calculatedIndex));
						}
						if (!temp.equals(currentTuple)) {
							previousPartitionStartPage = partitionStartPage;
							previousPartitionStartIndex = partitionStartIndex;

							partitionStartPage = pagePointer;
							partitionStartIndex = tupleIndex;
							previousTuple = currentTuple;
							currentTuple = temp;
						}
						tupleIndex++;
						return fot;
					} else {
						fin.close();
						closed = true;
						return null;
					}
				} else {
					fin.close();
					closed = true;
					return null;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Sets the reader to begin reading
	 * at a certain page and index in that page.
	 * @param pagePointer The page number
	 * @param tupleIndex The index of the tuple of the given page
	 */
	public void SetPageTuplePointer(int pagePointer, int tupleIndex) {
		this.tupleIndex = tupleIndex;
		this.pagePointer = pagePointer - 1;
		this.initializedPages = false;
	}
	

	/**
	 * Resets the reader to a specific page and index
	 * based on the 0-index absolute position of the tuple.
	 * I.e. if a tuple was on page 2, index 3, the absolute position
	 * would be 3*numbertuplesperpage + 4
	 * @param index 0-indexed tuple's absolute position in binary file
	 */
	public void reset(int index) {
		if (keyNames == null || keyNames.size() != tupleLength){
			System.out.println(
					"ERROR TUPLE LENGTH: " + tupleLength + "   KEY NAMES PROVIDED: " + (keyNames == null ? "null" : keyNames.size()));
		}
		try {
			if (!closed) {
				try {
					fin.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			fin = new FileInputStream(fileLocation);
			fc = fin.getChannel();
			pageInMemory = null;
			closed = false;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		int tuplesPerPage = (BUFFER_SIZE - METADATA_SIZE) / (tupleLength * BYTE_SIZE);
		int page_idx = index / tuplesPerPage;
		int tuple_idx = index % tuplesPerPage;
		if (page_idx < 0 || tuple_idx < 0) {
			System.out.println("PAUSE");
		}
		SetPageTuplePointer(page_idx, tuple_idx);
	}
	
	/**
	 * A function to easily convert an absolute tuple index
	 * into an RID for use with BPlusTree serialization.
	 * @param index Absolute tuple index (0-indexed)
	 * @return RID containing pageID and tupleID
	 */
	public RID absoluteIDToRID(int index){
		assert (tupleLength > 0);
		assert (index >= 0);
		int tuplesPerPage = (BUFFER_SIZE - METADATA_SIZE) / (tupleLength * BYTE_SIZE);
		int page_idx = index / tuplesPerPage;
		int tuple_idx = index % tuplesPerPage;
		assert(page_idx >= 0 && tuple_idx >= 0);
		return new RID(page_idx,tuple_idx);
	}
}
