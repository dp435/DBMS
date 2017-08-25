package LogicalOperator;

import LogicalOperator.*;

/**
 * Logical plan visitor interface
 * 
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */
public interface LogicalPlanVisitor {
	void visit(LogicalScanOperator node);

	void visit(LogicalIndexScanOperator node);
	
	void visit(LogicalSelectionOperator node);

	void visit(LogicalJoinOperator node);

	void visit(LogicalProjectionOperator node);

	void visit(LogicalSortOperator node);

	void visit(LogicalDuplicateEliminationOperator node);
}
