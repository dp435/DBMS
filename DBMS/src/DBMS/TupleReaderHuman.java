package DBMS;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

/**
 * Class that reads a human-readble tuple file.
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 *
 */
public class TupleReaderHuman {

	private String tableName;
	private String aliasName;
	private BufferedReader tableReader;
	private TableInfo currentTable;
	private FileInputStream fis;

	/**
	 * Constructor for TupleReaderHuman
	 * @param tableName The table name of the table this file refers to
	 * @param aliasName The alias name (if any) of the table the file refers to
	 * @param currentTable The relevant table-meta data for the table this file refers to
	 */
	public TupleReaderHuman(String tableName, String aliasName, TableInfo currentTable) {
		this.tableName = tableName;
		this.aliasName = aliasName;
		this.currentTable = currentTable;
		tableReader = null;
		try {
			fis = new FileInputStream(new File(currentTable.getLocation()));
			tableReader = new BufferedReader(new InputStreamReader(fis));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Constructor for only reading a binary tuple
	 * based on its file location.
	 * @param fileLocation The location of the binary file
	 */
	public TupleReaderHuman(String fileLocation) {
		tableReader = null;
		try {
			fis = new FileInputStream(new File(fileLocation));
			tableReader = new BufferedReader(new InputStreamReader(fis));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Read the next tuple from the referenced file.
	 * @return The next tuple from the file.
	 */
	public Tuple readTuple() {
		String line;
		try {
			if ((line = tableReader.readLine()) != null) {
				Hashtable<String, String> tupleData = new Hashtable<>();
				List<String> ordering = new ArrayList<>();
				String[] fields = line.split(",");
				FieldOrderedTuple fot = new FieldOrderedTuple();
				for (int i = 0; i < fields.length; i++) {
					if (aliasName != null && !aliasName.equals("")) {
						tupleData.put(aliasName + "." + currentTable.getFields().get(i), fields[i]);
						ordering.add(aliasName + "." + currentTable.getFields().get(i));
					} else {
						tupleData.put(tableName + "." + currentTable.getFields().get(i), fields[i]);
						ordering.add(tableName + "." + currentTable.getFields().get(i));
					}
				}
				return new Tuple(tableName, tupleData, ordering);
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
		String line;
		try {
			if ((line = tableReader.readLine()) != null) {
				Hashtable<String, String> tupleData = new Hashtable<>();
				String[] fields = line.split(",");
				FieldOrderedTuple fot = new FieldOrderedTuple();
				for (int i = 0; i < fields.length; i++) {
					fot.addField(Integer.parseInt(fields[i]));
				}
				return fot;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Set this reader back to its beginning marker.
	 */
	public void reset() {
		try {
			fis.getChannel().position(0);
			tableReader = new BufferedReader(new InputStreamReader(fis));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
