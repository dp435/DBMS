package PhysicalOperator;


import java.util.List;

import DBMS.Tuple;
import net.sf.jsqlparser.expression.Expression;

/**
 * This class is responsible for selecting tuples that satisfy the selection
 * condition.
 *
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class SelectionOperator extends Operator {

	public Operator scanner;
	private String tableName;
	public Expression selectionCondition;
	private String fromClause;

	/**
	 * Constructor for SelectionOperator.
	 * 
	 * @param fromClause
	 *            The from clause of the query. This indicates which table we
	 *            are currently working with.
	 * @param selectionCondition
	 *            The where clause of the query. This expression acts as the
	 *            selection condition.
	 * @param aliasName
	 *            The alias name for the current table. If none exists, then it
	 *            is set to null.
	 */
	public SelectionOperator(String fromClause, Expression selectionCondition, String aliasName, Operator child) {
		this.fromClause = fromClause;
		this.selectionCondition = selectionCondition;
		String[] fromList = fromClause.split(" ");
		tableName = fromList[0];
		scanner = child;
	}

	/**
	 * Method to get the next tuple satisfying the selection condition.
	 * 
	 * @return tuple satifying the selection condition.
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple currentTuple = null;
		boolean entryFound = false;
		while (!(entryFound)) {
			if ((currentTuple = scanner.getNextTuple()) != null) {
				EvaluateExpressionVisitor EV = new EvaluateExpressionVisitor(currentTuple);
				selectionCondition.accept(EV);
				if (EV.getResult() == true)
					entryFound = true;
			} else
				break;
		}
		return currentTuple;
	}

	/**
	 * Method to reset the state of the operator to start returning its output
	 * again from the beginning.
	 */
	@Override
	public void reset() {
		scanner.reset();

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
