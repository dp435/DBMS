package PhysicalOperator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import DBMS.BuildUnionFindVisitor;
import DBMS.DatabaseCatalog;
import DBMS.JavaUtils;
import DBMS.UFElement;
import DBMS.UnionFind;
import LogicalOperator.EvaluateJoinVisitor;
import LogicalOperator.LogicalJoinChild;
import LogicalOperator.LogicalJoinOperator;
import LogicalOperator.LogicalOperator;
import LogicalOperator.LogicalScanOperator;
import LogicalOperator.LogicalSelectionOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;

/**
 * This class constructs the join tree for the best
 * cost join using a bottom-up method.
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 *
 */
public class ConstructJoinTree {

	private JavaUtils utils;
	ArrayList<Expression> joinList;
	ArrayList<LogicalJoinChild> joinOrder;

	/**
	 * The constructor for the ConstructJoinTree.
	 * Does all logic for construction of tree upon creation.
	 * @param logicalTree The logical tree to be evaluated for
	 * creation of the optimal Physical Join Tree. 
	 */
	public ConstructJoinTree(LogicalJoinOperator logicalTree) {
		utils = new JavaUtils();
		List<LogicalJoinChild> joinChilds = logicalTree.getAllOperators();

		EvaluateJoinVisitor JV = new EvaluateJoinVisitor();
		if (logicalTree.joinCondition != null) {
			logicalTree.joinCondition.accept(JV);
		}
		joinList = JV.getJoinList();

		// First populate all joins of children
		for (LogicalJoinChild jc : joinChilds) {
			for (Expression exp : joinList) {
				if (utils.contains(exp.toString(), jc.getARName())) {
					jc.addJoinExpression(exp);
				}
			}
		}

		ArrayList<IntermediateJoinOperator> previousLevel = new ArrayList<>();
		Set<String> alreadyJoinedFirst = new HashSet<>();

		// Create the first level of joins
		for (LogicalJoinChild jc1 : joinChilds) {
			for (LogicalJoinChild jc2 : joinChilds) {
				// Don't add the tables twice
				String check = jc2.getARName().toString() + jc1.getARName().toString();
				if (jc1 != jc2 && !alreadyJoinedFirst.contains(check)) {
					// Use smaller relation as outer relation in join
					if (jc2.getNumTuples() > jc1.getNumTuples()) {
						previousLevel.add(joinFirst(jc1, jc2));
					} else {
						previousLevel.add(joinFirst(jc2, jc1));
					}
					alreadyJoinedFirst.add(jc1.getARName().toString() + jc2.getARName().toString());
				}
			}
		}

		ArrayList<IntermediateJoinOperator> upperLevel = previousLevel;
		// Do the rest of the levels.
		for (int i = 2; i < joinChilds.size(); i++) {
			previousLevel = (ArrayList<IntermediateJoinOperator>) upperLevel.clone();
			upperLevel = new ArrayList<>();

			for (LogicalJoinChild jc1 : joinChilds) {
				for (IntermediateJoinOperator op : previousLevel) {
					if (!op.joinedTables.contains(jc1.getARName())) {
						upperLevel.add(join(op, jc1));
					}
				}
			}
		}

		// Find the best join tree
		IntermediateJoinOperator best = new IntermediateJoinOperator(null, null);
		int bestCost = Integer.MAX_VALUE;
		for (int i = 0; i < upperLevel.size(); i++) {
			if (upperLevel.get(i).cost < bestCost) {
				bestCost = upperLevel.get(i).cost;
				best = upperLevel.get(i);
			}
		}

		// Build the join-order list
		IntermediateJoinOperator current = best;
		joinOrder = new ArrayList<>();
		while (current.leftChild instanceof IntermediateJoinOperator) {
			joinOrder.add(0, ((LogicalJoinChild) current.rightChild));
			current = (IntermediateJoinOperator) current.leftChild;
		}
		joinOrder.add(0, ((LogicalJoinChild) current.rightChild));
		joinOrder.add(0, ((LogicalJoinChild) current.leftChild));
	}

	public ArrayList<LogicalJoinChild> getJoinOrderList() {
		return joinOrder;
	}

	/**
	 * This function is to be called when joining two "base" tables i.e.
	 * LogicalJoinChild in this case.
	 * 
	 * @param d1
	 *            The left table
	 * @param d2
	 *            The right table
	 * @return an IntermediateJoinOperator that has these tables "joined"
	 */
	private IntermediateJoinOperator joinFirst(LogicalJoinChild d1, LogicalJoinChild d2) {
		// Need to set size, cost, and expressions
		IntermediateJoinOperator ijo = new IntermediateJoinOperator(d1, d2);
		int vNumerator = d2.getNumTuples() * d2.getNumTuples();
		int vDenominator = 1;

		// First table
		double fullReductionFactor = 1;
		for (String att : d1.getAllAttributes()) {
			fullReductionFactor *= calculateReductionFactor(d1, att);
		}
		int maxSize = (int) Math.max(1, (fullReductionFactor * d1.getNumTuples()));
		for (String att : d1.getAllAttributes()) {
			int getMin = d1.getMin(att);
			int getMax = d1.getMax(att);
			ijo.addToVHT(d1.getARName() + "." + att, Math.min(computeVTable(d1, att), maxSize));
		}

		// Second table
		fullReductionFactor = 1;
		for (String att : d2.getAllAttributes()) {
			fullReductionFactor *= calculateReductionFactor(d2, att);
		}
		maxSize = (int) Math.max(1, (fullReductionFactor * d2.getNumTuples()));
		for (String att : d2.getAllAttributes()) {
			ijo.addToVHT(d2.getARName() + "." + att, Math.min(computeVTable(d2, att), maxSize));
		}

		// Add equality expressions and all expressions for future use
		for (Expression exp : joinList) {
			if (utils.contains(exp.toString(), d1.getARName()) && utils.contains(exp.toString(), d2.getARName())) {
				ijo.addJoinExpression(exp);
				if (exp instanceof EqualsTo) {
					ijo.addEqualityJoinExpression(exp);
					ijo.addEqualityJoinList(exp);
				}
			}
		}

		// Find the minimal attributes of the unionelement the attribute is
		// found in
		BuildUnionFindVisitor vJoinMinimizer = new BuildUnionFindVisitor();
		if (ijo.equalityJoinExpression != null) {
			ijo.equalityJoinExpression.accept(vJoinMinimizer);
		}
		UnionFind unionData = vJoinMinimizer.getUnionedResult();

		for (String att : d1.getAllAttributes()) {
			UFElement e = unionData.find(d1.getARName() + "." + att);
			int min = Integer.MAX_VALUE;
			for (String vatt : e.attributes) {
				min = Math.min(ijo.getFromVHT(vatt), min);
			}
			ijo.addToJHT(d1.getARName() + "." + att, min);
		}

		for (String att : d2.getAllAttributes()) {
			UFElement e = unionData.find(d2.getARName() + "." + att);
			int min = Integer.MAX_VALUE;
			for (String vatt : e.attributes) {
				min = Math.min(ijo.getFromVHT(vatt), min);
			}
			ijo.addToJHT(d2.getARName() + "." + att, min);
		}

		// Do the actual join size calculation
		for (Expression exp : ijo.equalityJoinList) {
			String leftV = exp.toString().split(" = ")[0];
			String rightV = exp.toString().split(" = ")[1];
			vDenominator *= Math.max(ijo.getFromJHT(leftV), ijo.getFromJHT(rightV));
		}

		ijo.size = Math.max(1, vNumerator / vDenominator);
		ijo.joinedTables.add(d1.getARName());
		ijo.joinedTables.add(d2.getARName());
		return ijo;
	}

	/**
	 * This function is to be called for joining a third table with two
	 * previously joined tables.
	 * 
	 * @param d1
	 *            The left join tree
	 * @param d2
	 *            The right table
	 * @return an IntermediateJoinOperator that has these tree and table
	 *         "joined"
	 */
	@SuppressWarnings("unchecked")
	private IntermediateJoinOperator join(IntermediateJoinOperator d1, LogicalJoinChild d2) {
		// Need to set size, cost, and expressions
		IntermediateJoinOperator ijo = new IntermediateJoinOperator(d1, d2);
		int vNumerator = d1.size * d2.getNumTuples();
		int vDenominator = 1;

		// Add back in info from original tree
		// ijo.addEqualityJoinExpression(d1.equalityJoinExpression);
		// ijo.addJoinExpression(d1.joinExpression);
		// ijo.equalityJoinList.addAll(d1.equalityJoinList);
		ijo.joinedTables.addAll(d1.joinedTables);
		ijo.joinedTables.add(d2.getARName());
		ijo.vHTable = (Hashtable<String, Integer>) d1.vHTable.clone();
		ijo.joinHTable = (Hashtable<String, Integer>) d1.joinHTable.clone();

		// Joining table
		double fullReductionFactor = 1;
		fullReductionFactor = 1;
		for (String att : d2.getAllAttributes()) {
			fullReductionFactor *= calculateReductionFactor(d2, att);
		}
		int maxSize = (int) Math.max(1, (fullReductionFactor * d2.getNumTuples()));
		for (String att : d2.getAllAttributes()) {
			ijo.addToVHT(d2.getARName() + "." + att, Math.min(computeVTable(d2, att), maxSize));
			ijo.addToJHT(d2.getARName() + "." + att, Math.min(computeVTable(d2, att), maxSize));
		}

		// Add equality expressions and all expressions for future use
		for (Expression exp : joinList) {
			for (String relation : d1.joinedTables) {
				if (utils.contains(exp.toString(), relation) && utils.contains(exp.toString(), d2.getARName())) {
					ijo.addJoinExpression(exp);
					if (exp instanceof EqualsTo) {
						ijo.addEqualityJoinExpression(exp);
						ijo.addEqualityJoinList(exp);
					}
				}
			}
		}

		// Find the minimal attributes of the unionelement the attribute is
		// found in
		BuildUnionFindVisitor vJoinMinimizer = new BuildUnionFindVisitor();
		if (ijo.equalityJoinExpression != null) {
			ijo.equalityJoinExpression.accept(vJoinMinimizer);
		}
		UnionFind unionData = vJoinMinimizer.getUnionedResult();

		for (String att : d2.getAllAttributes()) {
			UFElement e = unionData.find(d2.getARName() + "." + att);
			int min = Integer.MAX_VALUE;
			for (String vatt : e.attributes) {
				min = Math.min(ijo.getFromJHT(vatt), min);
			}
			for (String vatt : e.attributes) {
				ijo.addToJHT(vatt, min);
			}
		}

		// Do the actual join size calculation
		for (Expression exp : ijo.equalityJoinList) {
			for (String relation : d1.joinedTables) {
				if (relation != d2.getARName() && utils.contains(exp.toString(), relation)
						&& utils.contains(exp.toString(), d2.getARName())) {
					String leftV = exp.toString().split(" = ")[0];
					String rightV = exp.toString().split(" = ")[1];
					vDenominator *= Math.max(ijo.getFromJHT(leftV), ijo.getFromJHT(rightV));
				}
			}
		}

		ijo.size = Math.max(1, vNumerator / vDenominator) + d1.size;
		ijo.cost = d1.cost + d1.size;

		return ijo;
	}

	/**
	 * This function computes the first/second case of computing V-values.
	 * 
	 * @param j1
	 *            Base table
	 * @param attribute
	 *            Attribute of base table
	 * @return max attribute value - min attribute value + 1
	 */
	private int computeVTable(LogicalJoinChild j1, String attribute) {
		if (j1.getOperator() instanceof LogicalScanOperator) {
			return j1.getMax(attribute) - j1.getMin(attribute) + 1;
		} else {
			assert (j1.getOperator() instanceof LogicalSelectionOperator);
			LogicalSelectionOperator lso = (LogicalSelectionOperator) j1.getOperator();
			double fullRange = j1.getMax(attribute) - j1.getMin(attribute) + 1;
			double reductionFactor = calculateReductionFactor(j1.getRelationName(), j1.getAliasName(), attribute,
					lso.selectionCondition);
			return (int) Math.max(Math.min(Math.floor(fullRange * reductionFactor), j1.getNumTuples()), 1);
		}
	}

	/**
	 * This function computes the reduction factor of an attribute in a table.
	 * 
	 * @param j1
	 *            Base table
	 * @param attribute
	 *            Attribute of base table
	 * @return Reduction factor.
	 */
	private double calculateReductionFactor(LogicalJoinChild j1, String attribute) {
		if (j1.getOperator() instanceof LogicalScanOperator) {
			return 1;
		} else {
			assert (j1.getOperator() instanceof LogicalSelectionOperator);
			LogicalSelectionOperator lso = (LogicalSelectionOperator) j1.getOperator();
			return calculateReductionFactor(j1.getRelationName(), j1.getAliasName(), attribute, lso.selectionCondition);
		}
	}

	/**
	 * This function computes the reduction factor of an attribute in a table.
	 * 
	 * @param j1
	 *            Base table
	 * @param attribute
	 *            Attribute of base table
	 * @return Reduction factor.
	 */
	
	/**
	 * This function computes the reduction factor of an attribute in a table.
	 * @param tableName The table name
	 * @param aliasName The alias name, if there is one
	 * @param attribute The attribute name
	 * @param selectionCondition The selection conditions that are used to find
	 * 			the reduction factor.
	  * @return Reduction factor.
	 */
	public double calculateReductionFactor(String tableName, String aliasName, String attribute,
			Expression selectionCondition) {
		DatabaseCatalog catalog = DatabaseCatalog.getInstance();
		Integer baseTableMinimum = catalog.getStatistics(tableName).getAttributeMinimum(attribute);
		Integer baseTableMaximum = catalog.getStatistics(tableName).getAttributeMaximum(attribute);
		Integer baseTableRange = baseTableMaximum - baseTableMinimum + 1;

		EvaluateIndexExpressionVisitor IV = new EvaluateIndexExpressionVisitor(tableName, aliasName, attribute);
		selectionCondition.accept(IV);
		Integer indexLow = IV.getLowkey();
		Integer indexHigh = IV.getHighkey();

		if (selectionCondition.toString().contains(" = ") && selectionCondition.toString().contains("." + attribute)) {
			indexLow = Integer.parseInt(selectionCondition.toString().split(" = ")[1]);
			indexHigh = Integer.parseInt(selectionCondition.toString().split(" = ")[1]);
		}

		if (indexLow != null && indexHigh != null)
			return (double) ((indexHigh - indexLow + 1.0) / baseTableRange);
		if (indexLow != null && indexHigh == null)
			return (double) ((baseTableMaximum - indexLow + 1.0) / baseTableRange);
		if (indexLow == null && indexHigh != null)
			return (double) ((indexHigh - baseTableMinimum + 1.0) / baseTableRange);
		else
			return 1.0;
	}

}
