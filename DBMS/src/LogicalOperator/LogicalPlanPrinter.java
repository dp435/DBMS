package LogicalOperator;

import java.util.ArrayList;
import java.util.List;

import DBMS.BuildUnionFindVisitor;
import DBMS.JavaUtils;
import DBMS.UFElement;
import DBMS.UnionFind;
import net.sf.jsqlparser.expression.Expression;

/**
 * Visitor to traverse through a logical operator tree to print the logical
 * plan. Pre-order tree traversal is used.
 * 
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */
public class LogicalPlanPrinter implements LogicalPlanVisitor {
	StringBuilder output;
	int depth;

	/** Constructor for LogicalPlanPrinter */
	public LogicalPlanPrinter() {
		output = new StringBuilder();
		depth = 0;
	}

	/**
	 * Method to return a String representing the logical plan.
	 * 
	 * @return String representing logical plan.
	 */
	public String printLogicalTree() {
		return output.toString();
	}

	/**
	 * Visit method for LogicalScanOperator node.
	 * 
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(LogicalScanOperator node) {
		output.append(new String(new char[depth]).replace("\0", "-"));
		output.append("Leaf[" + node.tableName + "]\n");
	}

	/**
	 * Visit method for LogicalIndexScanOperator node.
	 * 
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(LogicalIndexScanOperator node) {
		System.out.println("WARNING: a deprecated operator was called.");
	}

	/**
	 * Visit method for LogicalSelectionOperator node.
	 * 
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(LogicalSelectionOperator node) {
		output.append(new String(new char[depth]).replace("\0", "-"));
		output.append("Select[" + node.selectionCondition + "]\n");
		depth++;
		node.scanner.accept(this);
		depth--;
	}

	/**
	 * Visit method for LogicalJoinOperator node.
	 * 
	 * Visits its child from left-to-right.
	 * 
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(LogicalJoinOperator node) {
		output.append(new String(new char[depth]).replace("\0", "-"));
		JavaUtils utils = new JavaUtils();
		Expression residualJoins = (node.residualJoins != null) ? utils.listToExpression(node.residualJoins) : null;
		UnionFind unionInfo = node.getUnionFindDatabase();

		output.append("Join[" + residualJoins + "]\n");

		List<String> printHistory = new ArrayList<String>();
		if (node.getUnionFindDatabase() != null)
			for (UFElement unionData : node.getUnionFindDatabase().getElements()) {

				UFElement firstUnionAttribute = unionInfo.find(unionData.attributes.get(0));
				output.append(
						"[" + unionData.attributes + ", equals " + firstUnionAttribute.equalityConstraint + ", min "
								+ firstUnionAttribute.lowerBound + ", max " + firstUnionAttribute.upperBound + "]\n");
			}

		depth++;
		for (LogicalJoinChild jc : node.getAllOperators()) {
			jc.getOperator().accept(this);
		}
	}

	/**
	 * Visit method for LogicalProjectionOperator node.
	 * 
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(LogicalProjectionOperator node) {
		output.append(new String(new char[depth]).replace("\0", "-"));
		output.append("Project" + node.projectionCondition.toString() + "\n");
		depth++;
		node.childOperator.accept(this);
	}

	/**
	 * Visit method for LogicalSortOperator node.
	 * 
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(LogicalSortOperator node) {
		output.append(new String(new char[depth]).replace("\0", "-"));
		if (node.printOrder != null)
			output.append("Sort" + node.printOrder.toString() + "\n");
		else
			output.append("Sort[]\n");
		depth++;
		node.childOperator.accept(this);
	}

	/**
	 * Visit method for LogicalDuplicateEliminationOperator node.
	 * 
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(LogicalDuplicateEliminationOperator node) {
		output.append(new String(new char[depth]).replace("\0", "-"));
		output.append("DupElim\n");
		depth++;
		node.childOperator.accept(this);
	}

}
