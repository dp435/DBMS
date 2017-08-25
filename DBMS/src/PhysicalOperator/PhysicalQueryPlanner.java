package PhysicalOperator;

import LogicalOperator.LogicalOperator;
import LogicalOperator.LogicalPlanPrinter;
import LogicalOperator.LogicalQueryPlanner;

public class PhysicalQueryPlanner {

	private Operator physicalPlan;

	/**
	 * Constructor for PhysicalQueryPlanner.
	 * 
	 * @param logicalPlan
	 *            the logical plan of the current query.
	 */
	public PhysicalQueryPlanner(LogicalQueryPlanner logicalPlan) {
		LogicalOperator rootLogicalOperator = logicalPlan.constructLogicalQueryPlan();
		PhysicalPlanBuilder LPV = new PhysicalPlanBuilder();
		rootLogicalOperator.accept(LPV);
		physicalPlan = LPV.getResult();
	}

	/** Method to fetch the constructed Operator tree. */
	public Operator getPhysicalPlan() {
		return physicalPlan;
	}

}
