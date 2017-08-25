package PhysicalOperator;

public interface PhysicalPlanVisitor {

	void visit(BNLJoinOperator node);

	void visit(DuplicateEliminationOperator node);

	void visit(ExternalSortOperator node);

	void visit(IndexScanOperator node);

	void visit(ScanOperator node);

	void visit(SelectionOperator node);

	void visit(SMJOperator node);

	void visit(SortOperator node);

	void visit(TNLJoinOperator node);

	void visit(ProjectionOperator node);

}
