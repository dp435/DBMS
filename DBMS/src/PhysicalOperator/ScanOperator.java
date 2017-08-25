package PhysicalOperator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Hashtable;
import DBMS.*;

/**
 * This class is responsible for scanning the data file for the specified table
 * and returning tuples representing each line of the data file.
 *
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class ScanOperator extends Operator {

	private TableInfo currentTable;
	private TupleReaderBinary tupleReaderBinary;
	private TupleReaderHuman tupleReaderHuman;
	public String tableName;

	/**
	 * Constructor for ScanOperator.
	 * 
	 * @param tableName
	 *            name of current table being scanned.
	 * @param aliasName
	 *            The alias name for the current table. If none exists, then it
	 *            is set to null.
	 */
	public ScanOperator(String tableName, String aliasName) {
		DatabaseCatalog catalog = DatabaseCatalog.getInstance();
		currentTable = catalog.GetTable(tableName);
		tupleReaderBinary = new TupleReaderBinary(tableName, aliasName, currentTable);
		TableInfo currentTableHuman = new TableInfo(currentTable.getLocation(), currentTable.getFields());
		tupleReaderHuman = new TupleReaderHuman(tableName, aliasName, currentTableHuman);
		this.tableName = tableName;
	}

	/**
	 * Method to get a tuple representing the next line of the data file for the
	 * specified table.
	 * 
	 * @return tuple representing the next entry of the data table.
	 */
	@Override
	public Tuple getNextTuple() {
		// Tuple ret = tupleReaderHuman.readTuple();
		Tuple ret = tupleReaderBinary.readTuple(false);
		return ret;
	}

	/**
	 * Method to reset the state of the operator to start returning its output
	 * again from the beginning.
	 */
	@Override
	public void reset() {
		tupleReaderHuman.reset();
		tupleReaderBinary.reset();
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
