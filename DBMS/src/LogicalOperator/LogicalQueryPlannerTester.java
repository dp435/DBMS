package LogicalOperator;

import static org.junit.Assert.*;

import org.junit.Test;

import DBMS.Interpreter;

public class LogicalQueryPlannerTester {

	@Test
	public void test() {

		//Interpreter interpreter = new Interpreter("./sandbox", "./output", "./temp", false, false);
		Interpreter interpreter = new Interpreter("./benchmark2", "./output", "./temp", false, false);
		interpreter.readline();
		LogicalQueryPlanner LQP = new LogicalQueryPlanner(interpreter);
		LQP.constructLogicalQueryPlan();
	}

}
