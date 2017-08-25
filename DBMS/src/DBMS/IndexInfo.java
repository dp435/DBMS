package DBMS;

import java.util.List;

/**
 * This class is responsible for storing information about the index information
 * of each table.
 * 
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 *
 */
public class IndexInfo {
	private String relationName;
	private String attributeName;
	private boolean isClustered;
	private int treeOrder;

	/**
	 * Constructor for IndexInfo.
	 * 
	 * @param relationName
	 * @param fields
	 */
	public IndexInfo(String relationName, String attributeName, boolean isClustered, int treeOrder) {
		this.relationName = relationName;
		this.attributeName = attributeName;
		this.isClustered = isClustered;
		this.treeOrder = treeOrder;
	}

	/**
	 * Method to get the relation.
	 * 
	 * @return the table name in the relation.
	 */
	public String getRelation() {
		return relationName;
	}

	/**
	 * Method to get the attribute that the relation is indexed on.
	 * 
	 * @return the attribute name that the relation is indexed on.
	 */
	public String getAttribute() {
		return attributeName;
	}

	/**
	 * Method to see if current indexed relation is clustered.
	 * 
	 * @return true if clustered; false otherwise.
	 */
	public boolean getIsClustered() {
		return isClustered;
	}

	/**
	 * Method to get the tree order of the indexed relation.
	 * 
	 * @return the tree order of the indexed relation.
	 */
	public int getTreeOrder() {
		return treeOrder;
	}

}
