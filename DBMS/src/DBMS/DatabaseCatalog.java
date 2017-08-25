package DBMS;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

/**
 * Singleton Class To Return A DatabaseCatalog
 * 
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 *
 */
public final class DatabaseCatalog {
	private static final DatabaseCatalog instance = new DatabaseCatalog();
	private Hashtable<String, TableInfo> tableLookup;
	private String inputDirectory;
	private String outputDirectory;
	private String tempDirectory;
	private String indexDirectory;
	private List<IndexInfo> indexInfoTable;
	private List<String> currentRelations;
	private Hashtable<String, ArrayList<String>> attributesTable;
	private Hashtable<String, StatisticsInfo> statsTable;
	private Hashtable<String, Integer> bPlusTreeInfoTable;
	private Hashtable<String, Boolean> isClusteredInfoTable;
	private Hashtable<String, Integer> treeOrderInfoTable;

	/**
	 * Private method used by the class to instantiate the one and only
	 * DatabaseCatalog.
	 */
	private DatabaseCatalog() {
		tableLookup = new Hashtable<>();
		indexInfoTable = new ArrayList<IndexInfo>();
		currentRelations = new ArrayList<String>();
		attributesTable = new Hashtable<String, ArrayList<String>>();
		statsTable = new Hashtable<String, StatisticsInfo>();
		bPlusTreeInfoTable = new Hashtable<String, Integer>();
		isClusteredInfoTable = new Hashtable<String, Boolean>();
		treeOrderInfoTable = new Hashtable<String, Integer>();
	}

	/**
	 * Method to set the input, output, temp directories.
	 * 
	 * @param input
	 *            the path to the input directory
	 * @param output
	 *            the path to the output directory
	 * @param temp
	 *            the path to the temporary directory
	 */
	public void setDirectory(String input, String output, String temp) {
		inputDirectory = input;
		outputDirectory = output;
		tempDirectory = temp;
	}

	/**
	 * Method to set the index directory.
	 * 
	 * @param index
	 *            the path to the index directory
	 */
	public void setIndexDirectory(String index) {
		indexDirectory = index;
	}

	/**
	 * Return the input directory.
	 * 
	 * @return String representing path to input directory.
	 */
	public String getInputDirectory() {
		return inputDirectory;
	}

	/**
	 * Return the output directory.
	 * 
	 * @return String representing path to output directory.
	 */
	public String getOutputDirectory() {
		return outputDirectory;
	}

	/**
	 * Return the temp directory.
	 * 
	 * @return String representing path to temp directory.
	 */
	public String getTempDirectory() {
		return tempDirectory;
	}

	/**
	 * Return the inddex directory.
	 * 
	 * @return String representing path to temp directory.
	 */
	public String getIndexDirectory() {
		return indexDirectory;
	}

	/**
	 * Set a given tablename's tableInfo.
	 * 
	 * @param tablename
	 * @param tableInfo
	 */
	public void SetTable(String tablename, TableInfo tableInfo) {
		tableLookup.put(tablename, tableInfo);
	}

	/**
	 * Get a given tablename's tableInfo.
	 * 
	 * @param tablename
	 * @return tableInfo for this tablename.
	 */
	public TableInfo GetTable(String tablename) {
		return tableLookup.get(tablename);
	}

	/**
	 * Method to return the single instance of this DatabaseCatalog.
	 * 
	 * @return This DatabaseCatalog.
	 */
	public static DatabaseCatalog getInstance() {
		return instance;
	}

	/**
	 * Method to add an entry to the index info table.
	 * 
	 * @param info
	 *            The info to be added.
	 */
	public void addIndexInfo(IndexInfo info) {
		indexInfoTable.add(info);
		isClusteredInfoTable.put(info.getRelation() + "." + info.getAttribute(), info.getIsClustered());
		treeOrderInfoTable.put(info.getRelation() + "." + info.getAttribute(), info.getTreeOrder());
	}

	/**
	 * Method to get the index info table.
	 * 
	 * @return the current indexInfoTable
	 */
	public List<IndexInfo> getIndexInfoTable() {
		return indexInfoTable;
	}

	/**
	 * Method to check if an indexed attribute for a relation is clustered or
	 * not.
	 * 
	 * @return true if clustered; else, false.
	 */
	public Boolean checkIfClustered(String relationName, String attributeName) {
		return isClusteredInfoTable.get(relationName + "." + attributeName);
	}

	/**
	 * Method to get the tree order of an indexed attribute.
	 * 
	 * @return the tree order for an indexed attribute.
	 */
	public Integer getTreeOrder(String relationName, String attributeName) {
		return treeOrderInfoTable.get(relationName + "." + attributeName);
	}

	/**
	 * Method to check if relation is indexed.
	 * 
	 * @return true if index; false, otherwise.
	 */
	public boolean isIndexed(String tablename, String attributename) {
		for (IndexInfo info : indexInfoTable) {
			if (tablename.equals(info.getRelation()) && attributename.equals(info.getAttribute()))
				return true;
		}
		return false;
	}

	/**
	 * Method to add the base table to the relation data base.
	 * 
	 * @param relationName
	 *            the name of the base table.
	 */
	public void addRelation(String relationName) {
		currentRelations.add(relationName);
	}

	/**
	 * Method to get a list of all base relations.
	 * 
	 * @return a list of base relations.
	 */
	public List<String> getAllRelations() {
		return currentRelations;
	}

	/**
	 * Method to add attribute info for a specific relation.
	 * 
	 * @param relation
	 *            the name of the base relation
	 * @param attributeList
	 *            the list of attributes associated with the base relation.
	 */
	public void addAttributeInfo(String relation, ArrayList<String> attributeList) {
		attributesTable.put(relation, attributeList);
	}

	/**
	 * Method to get a list of attributes associated with relation.
	 * 
	 * @param relation
	 *            the relation that we are looking up its attribute info for.
	 * @return a list of attributes associated with the input relation.
	 */
	public ArrayList<String> getAttributeInfo(String relation) {
		return attributesTable.get(relation);
	}

	/**
	 * Method to add statistics info for a relation.
	 * 
	 * @param relation
	 *            the relation that we are adding statistics for
	 * @param stats
	 *            the statistics for the relation.
	 */
	public void addStatistics(String relation, StatisticsInfo stats) {
		statsTable.put(relation, stats);
	}

	/**
	 * Method to get the statistics info for a relation.
	 * 
	 * @param relation
	 *            the relation that we are looking up the statistics for.
	 * @return the statistics of the input relation.
	 */
	public StatisticsInfo getStatistics(String relation) {
		return statsTable.get(relation);
	}

	/**
	 * Method to add B+ tree info of a particular relation attribute.
	 * 
	 * @param relation
	 *            the name of the base relation.
	 * @param attribute
	 *            the name of the attribute
	 * @param numLeaves
	 *            the number of leaves in the B+ tree for the particular
	 *            relation attribute.
	 */
	public void addNumLeaves(String relation, String attribute, Integer numLeaves) {
		bPlusTreeInfoTable.put(relation + "." + attribute, numLeaves);
	}

	/**
	 * Method to get number of leaves in the B+ tree of the input relation
	 * attribute.
	 * 
	 * @return the number of leaves in the B+ tree of the input relation
	 *         attribute.
	 */
	public Integer getNumLeaves(String relation, String attribute) {
		return bPlusTreeInfoTable.get(relation + "." + attribute);
	}
}
