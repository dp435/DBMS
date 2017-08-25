package PhysicalOperator;

import DBMS.JavaUtils;
import net.sf.jsqlparser.expression.Expression;

/**
 * Visitor to traverse through a physical operator tree to print the physical
 * plan. Pre-order tree traversal is used.
 * 
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class PhysicalPlanPrinter implements PhysicalPlanVisitor {

	StringBuilder output;
	int depth;

	/** Constructor for PhysicalPlanPrinter */
	public PhysicalPlanPrinter() {
		output = new StringBuilder();
		depth = 0;
	}

	/**
	 * Method to return a String representing the physical plan.
	 * 
	 * @return String representing physical plan.
	 */
	public String printPhysicalTree() {
		return output.toString();
	}

	/**
	 * Visit method for BNLJoinOperator node.
	 * 
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(BNLJoinOperator node) {
		output.append(new String(new char[depth]).replace("\0", "-"));
		output.append("BNLJ[" + node.exp + "]\n");
		depth++;
		node.leftChild.accept(this);
		node.rightChild.accept(this);
		depth--;
	}

	/**
	 * Visit method for DuplicateEliminationOperator node.
	 * 
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(DuplicateEliminationOperator node) {
		output.append(new String(new char[depth]).replace("\0", "-"));
		output.append("DupElim\n");
		depth++;
		node.sorter.accept(this);
	}

	/**
	 * Visit method for ExternalSortOperator node.
	 * 
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(ExternalSortOperator node) {
		output.append(new String(new char[depth]).replace("\0", "-"));

		if (node.printOrder != null) {
			output.append("ExternalSort" + node.printOrder.toString() + "\n");
		} else
			output.append("ExternalSort[]\n");
		depth++;
		node.childOperator.accept(this);
		depth--;
	}

	/**
	 * Visit method for IndexScanOperator node.
	 * 
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(IndexScanOperator node) {
		output.append(new String(new char[depth]).replace("\0", "-"));
		output.append("IndexScan[" + node.tableName + "," + node.indexedAttribute + "," + node.lowerBound + ","
				+ node.upperBound + "]\n");
	}

	/**
	 * Visit method for IndexScanOperator node.
	 * 
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(ScanOperator node) {
		output.append(new String(new char[depth]).replace("\0", "-"));
		output.append("TableScan[" + node.tableName + "]\n");

	}

	/**
	 * Visit method for SelectionOperator node.
	 * 
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(SelectionOperator node) {
		output.append(new String(new char[depth]).replace("\0", "-"));
		output.append("Select[" + node.selectionCondition + "]\n");
		depth++;
		node.scanner.accept(this);
		depth--;

	}

	/**
	 * Visit method for SMJOperator node.
	 * 
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(SMJOperator node) {
		output.append(new String(new char[depth]).replace("\0", "-"));
		JavaUtils utils = new JavaUtils();
		Expression joinCondition = utils.listToExpression(node.conditionList);
		output.append("SMJ[" + joinCondition + "]\n");
		depth++;
		node.innerChild.accept(this);
		node.outerChild.accept(this);
		depth--;
	}

	/**
	 * Visit method for SortOperator node.
	 * 
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(SortOperator node) {
		output.append(new String(new char[depth]).replace("\0", "-"));
		if (node.printOrder != null)
			output.append("InternalSort" + node.printOrder.toString() + "\n");
		else
			output.append("InternalSort[]\n");
		depth++;
		node.childOperator.accept(this);
		depth--;
	}

	/**
	 * Visit method for TNLJoinOperator node.
	 * 
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(TNLJoinOperator node) {
		output.append(new String(new char[depth]).replace("\0", "-"));
		output.append("TNLJ[" + node.exp + "]\n");
		depth++;
		node.leftChild.accept(this);
		node.rightChild.accept(this);
		depth--;
	}

	/**
	 * Visit method for ProjectionOperator node.
	 * 
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(ProjectionOperator node) {
		output.append(new String(new char[depth]).replace("\0", "-"));
		output.append("Project" + node.selectClause.toString() + "\n");
		depth++;
		node.child.accept(this);
	}

}
