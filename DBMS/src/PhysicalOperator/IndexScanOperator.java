package PhysicalOperator;

import DBMS.*;

/**
 * This class is responsible for scanning the data file for the specified table
 * and returning tuples representing each line of the data file. Does so using
 * an index backed by a B+ Tree.
 *
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class IndexScanOperator extends Operator {

	public String tableName;
	private TableInfo currentTable;
	private String fieldPrefix;
	private TupleReaderBinary tupleReaderBinary;

	Integer lowerBound;
	Integer upperBound;
	BPlusTreeReader bPlusTreeReader;

	// Whether the index at the indexLocation is clustered
	boolean isClustered;

	// Location of the B+ Tree Index
	String indexLocation;

	String indexedAttribute;

	boolean readerInitialized;

	/**
	 * Constructor for IndexScanOperator.
	 * 
	 * @param tableName
	 *            name of current table being scanned.
	 * @param aliasName
	 *            The alias name for the current table. If none exists, then it
	 *            is set to null.
	 * @param lowerBound
	 *            the starting key of the indexed field. (Inclusive)
	 * @param upperBound
	 *            the ending key of the indexed field. (Inclusive)
	 */
	public IndexScanOperator(String tableName, String aliasName, String indexedAttribute, boolean isClustered,
			Integer lowerBound, Integer upperBound) {
		this.tableName = tableName;
		DatabaseCatalog catalog = DatabaseCatalog.getInstance();
		currentTable = catalog.GetTable(tableName);

		this.isClustered = isClustered;
		this.indexedAttribute = indexedAttribute;

		indexLocation = catalog.getIndexDirectory() + "/" + tableName + "." + indexedAttribute;

		fieldPrefix = tableName;
		if (aliasName != null && !aliasName.equals("")) {
			fieldPrefix = aliasName;
		}

		tupleReaderBinary = new TupleReaderBinary(tableName, aliasName, currentTable);

		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
		bPlusTreeReader = new BPlusTreeReader(indexLocation);
		if (lowerBound == null) {
			bPlusTreeReader.setNoLowerBound();
		} else {
			bPlusTreeReader.setLowerBound(lowerBound);
		}

		if (upperBound == null) {
			bPlusTreeReader.setNoUpperBound();
		} else {
			bPlusTreeReader.setUpperBound(upperBound);
		}
		readerInitialized = false;
	}

	/**
	 * Method to get a tuple representing the next line of the data file for the
	 * specified table.
	 * 
	 * @return tuple representing the next entry of the data table.
	 */
	@Override
	public Tuple getNextTuple() {
		if (readerInitialized && isClustered) {
			Tuple nextTuple = tupleReaderBinary.readTuple(false);
			if (nextTuple == null) {
				return null;
			}
			if (upperBound == null
					|| Integer.parseInt(nextTuple.getField(fieldPrefix + "." + indexedAttribute)) <= upperBound) {
				return nextTuple;
			} else {
				return null;
			}
		} else {
			RID returnedRID = bPlusTreeReader.getNextRID();
			if (returnedRID == null) {
				return null;
			}
			tupleReaderBinary.SetPageTuplePointer(returnedRID.pageID, returnedRID.tupleID);
			readerInitialized = true;
			return tupleReaderBinary.readTuple(false);
		}
	}

	/**
	 * Method to reset the state of the operator to start returning its output
	 * again from the beginning.
	 */
	@Override
	public void reset() {
		bPlusTreeReader = new BPlusTreeReader(indexLocation);
		if (lowerBound == null) {
			bPlusTreeReader.setNoLowerBound();
		} else {
			bPlusTreeReader.setLowerBound(lowerBound);
		}

		if (upperBound == null) {
			bPlusTreeReader.setNoUpperBound();
		} else {
			bPlusTreeReader.setUpperBound(upperBound);
		}
		readerInitialized = false;
	}

	/**
	 * Method to reset the state of the operator to start returning its output
	 * from the specified index. If the Operator is not a sort operator, this
	 * function does nothing.
	 */
	@Override
	public void reset(int index) {
	}

	/**
	 * Method for accepting visitor; just calls back visitor. Visitor method
	 * uses postorder traversal.
	 * 
	 * @param visitor
	 *            visitor to be accepted
	 */
	public void accept(PhysicalPlanVisitor visitor) {
		visitor.visit(this);
	}

}
