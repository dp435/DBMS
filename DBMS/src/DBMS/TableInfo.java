package DBMS;

import java.util.List;

/**
 * This class is responsible for storing information
 * about the fieldnames for a given tableLocation.
 * 
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 *
 */
public class TableInfo {
	private String tableLocation;
	private List<String> fields;

	/** Initialize a TableInfo with a given tableLocation
	 * and the relevant table field names.
	 * 
	 * @param tableLocation
	 * @param fields
	 */
	public TableInfo(String tableLocation, List<String> fields) {
		this.tableLocation = tableLocation;
		this.fields = fields;
	}
	
	/**
	 * 
	 * @return The location of this table
	 * on the disk.
	 */
	public String getLocation() {
		return tableLocation;
	}
	
	/**
	 * 
	 * @return The field names this
	 * table has.
	 */
	public List<String> getFields() {
		return fields;
	}
}