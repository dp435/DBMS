package DBMS;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import PhysicalOperator.ScanOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.PlainSelect;

/**
 * This class is responsible for processing and storing the schema information,
 * reading the queries file and parsing the query itself.
 *
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class Interpreter {

	public String inputDirectory;
	public String outputDirectory;
	public String tempDirectory;
	public boolean buildIndexes;
	public boolean evaluateQueries;
	String queriesFile;
	CCJSqlParser parser;

	public List selectClause;
	public FromItem fromClause;
	public List<Join> fromClauseRest;
	public Expression whereClause;
	public boolean distinct;
	public List<String> orderByList;

	public int joinBuffer;
	public String sortMethod;
	public int sortBuffer;

	public boolean EOF; // boolean value specifying if end of the queries.sql
						// file was reached.
	public boolean usingAliases;
	public List<String> outputOrder;

	public boolean useIndexes;

	private DatabaseCatalog catalog;

	/**
	 * Constructor for Interpreter.
	 * 
	 * This constructor determines the absolute path to the queries.sql file,
	 * and processes and stores the schema information to the database catalog.
	 * 
	 * @param inputDirectory
	 *            absolute path to the inputDirectory
	 * @param outputDirectory
	 *            absolute path to the outputDirectory
	 */
	public Interpreter(String inputDirectory, String outputDirectory, String tempDirectory, boolean buildIndexes,
			boolean evaluateQueries) {

		catalog = DatabaseCatalog.getInstance();
		EOF = false;
		queriesFile = inputDirectory + "/queries.sql";

		this.inputDirectory = inputDirectory;
		this.outputDirectory = outputDirectory;
		this.tempDirectory = tempDirectory;
		this.buildIndexes = buildIndexes;
		this.evaluateQueries = evaluateQueries;

		joinBuffer = 5;
		sortMethod = "EXTERNAL";
		sortBuffer = 5;

		catalog.setDirectory(inputDirectory, outputDirectory, tempDirectory);

		processSchemaInfo();

		processIndexInfo();

		collectStatistics();

	}

	/** Method to process schema info. */
	private void processSchemaInfo() {

		try {
			parser = new CCJSqlParser(new FileReader(queriesFile));
		} catch (Exception e) {
			e.printStackTrace();
		}

		String schemaPath = inputDirectory + "/db/schema.txt";
		BufferedReader schemaReader = null;
		try {
			schemaReader = new BufferedReader(new FileReader(new File(schemaPath)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		try {
			String line;
			while ((line = schemaReader.readLine()) != null) {
				String[] tableInfo = line.split(" ");
				ArrayList<String> fields = new ArrayList<>();
				for (int i = 1; i < tableInfo.length; i++) {
					fields.add(tableInfo[i]);
				}

				TableInfo currentTable = new TableInfo(inputDirectory + "/db/data/" + tableInfo[0], fields);
				catalog.SetTable(tableInfo[0], currentTable);
				catalog.addRelation(tableInfo[0]);
				catalog.addAttributeInfo(tableInfo[0],
						new ArrayList<String>(Arrays.asList(Arrays.copyOfRange(tableInfo, 1, tableInfo.length))));
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/** Method to process index info. */
	private void processIndexInfo() {
		String indexDirectory = inputDirectory + "/db/indexes";
		catalog.setIndexDirectory(indexDirectory);

		String indexInfoPath = inputDirectory + "/db/index_info.txt";
		BufferedReader indexInfoReader = null;
		try {
			indexInfoReader = new BufferedReader(new FileReader(new File(indexInfoPath)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		try {
			String line;
			while ((line = indexInfoReader.readLine()) != null) {
				String[] indexInfo = line.trim().split(" ");
				String relation = indexInfo[0];
				String attribute = indexInfo[1];

				for (int i = 2; i < indexInfo.length; i += 2) {
					boolean isClustered = false;
					if (indexInfo[i].equals("1"))
						isClustered = true;
					int treeOrder = Integer.parseInt(indexInfo[i + 1]);

					catalog.addIndexInfo(new IndexInfo(relation, attribute, isClustered, treeOrder));

					if (buildIndexes) {
						BPlusTree bp = new BPlusTree(relation, attribute, catalog.GetTable(relation).getFields(),
								isClustered, treeOrder);
						bp.constructTree();
						bp.checkConstraints();
						BPlusTreeWriter bptw = new BPlusTreeWriter(bp,
								catalog.getIndexDirectory() + "/" + relation + "." + attribute);
						bptw.writeTree();
					}

					BPlusTreeReader bPlusTreeReader = new BPlusTreeReader(
							catalog.getIndexDirectory() + "/" + relation + "." + attribute);
					bPlusTreeReader.readPageIntoMemory(0);
					bPlusTreeReader.deserializeHeaderInMemory();
					catalog.addNumLeaves(relation, attribute, bPlusTreeReader.getNumLeaves());

				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/** Method to collect statistics of each relation. */
	private void collectStatistics() {
		StringBuilder statsInfo = new StringBuilder();

		for (String relation : catalog.getAllRelations()) {
			StatisticsInfo stats = new StatisticsInfo(relation);
			ScanOperator scanner = new ScanOperator(relation, null);
			Tuple currentTuple;
			while ((currentTuple = scanner.getNextTuple()) != null) {
				stats.updateStats(currentTuple);
			}
			catalog.addStatistics(relation, stats);
			statsInfo.append(relation + " " + stats.getCount());
			List<String> attributeList = catalog.getAttributeInfo(relation);
			for (String attr : attributeList) {
				statsInfo.append(
						" " + attr + "," + stats.getAttributeMinimum(attr) + "," + stats.getAttributeMaximum(attr));
			}
			statsInfo.append("\n");
		}

		try (Writer writer = new BufferedWriter(
				new OutputStreamWriter(new FileOutputStream(inputDirectory + "/db/stats.txt"), "utf-8"))) {
			writer.write(statsInfo.toString());
			writer.close();
		} catch (Exception e) {
			System.out.println("Error encountered while saving statistics information.");
		}

	}

	/**
	 * Method to read and parse the next query in the queries file.
	 */
	public void readline() {
		usingAliases = false;
		try {
			Statement statement;
			if ((statement = parser.Statement()) != null) {
				Select select = (Select) statement;
				selectClause = ((PlainSelect) select.getSelectBody()).getSelectItems();
				fromClause = ((PlainSelect) select.getSelectBody()).getFromItem();
				fromClauseRest = ((PlainSelect) select.getSelectBody()).getJoins();
				whereClause = ((PlainSelect) select.getSelectBody()).getWhere();

				if (((PlainSelect) select.getSelectBody()).getDistinct() == null)
					distinct = false;
				else
					distinct = true;

				orderByList = null;
				if (((PlainSelect) select.getSelectBody()).getOrderByElements() != null) {
					orderByList = new ArrayList<String>();
					for (Object item : ((PlainSelect) select.getSelectBody()).getOrderByElements()) {
						orderByList.add(item.toString());
					}
				}
			} else {
				EOF = true;

				selectClause = null;
				fromClause = null;
				fromClauseRest = null;
				whereClause = null;
				orderByList = null;
				return;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		String[] fromList = fromClause.toString().split(" ");
		if (fromList.length > 1) {
			usingAliases = true;
		}
		if (selectClause.get(0).toString() == "*") {
			if (usingAliases) {
				outputOrder = new ArrayList<String>(catalog.GetTable(fromList[0]).getFields());
				outputOrder.replaceAll(fieldname -> fromList[2] + "." + fieldname);
			} else {
				outputOrder = new ArrayList<String>(catalog.GetTable(fromList[0]).getFields());
				outputOrder.replaceAll(fieldname -> fromList[0] + "." + fieldname);
			}
			if (fromClauseRest != null) {
				for (Join joinItem : fromClauseRest) {
					String[] fromExpression = joinItem.toString().split(" ");
					List<String> fieldnameList;
					if (usingAliases) {
						fieldnameList = new ArrayList<String>(catalog.GetTable(fromExpression[0]).getFields());
						fieldnameList.replaceAll(fieldname -> fromExpression[2] + "." + fieldname);
					} else {
						fieldnameList = new ArrayList<String>(catalog.GetTable(fromExpression[0]).getFields());
						fieldnameList.replaceAll(fieldname -> fromExpression[0] + "." + fieldname);
					}
					outputOrder.addAll(fieldnameList);
				}
			}
		} else {
			outputOrder = new ArrayList<String>();
			for (Object selectItem : selectClause) {
				outputOrder.add(selectItem.toString());
			}
		}
	}

	/**
	 * Method to switch queries file.
	 * 
	 * NOTE: to be used for testing purposes only.
	 */
	public void switchFile(String filepath) {
		queriesFile = filepath;
		try {
			parser = new CCJSqlParser(new FileReader(queriesFile));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
