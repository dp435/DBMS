package DBMS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

public class JavaUtils {

	/**
	 * Method to see if an expression contains specific table/alias name.
	 *
	 * @param exp
	 *            The expression to be checked.
	 * @param name
	 *            The name of table/alias.
	 * @return true if expression contains the name; false otherwise.
	 */
	public boolean contains(String exp, String name) {
		String[] condition = exp.toString().split("[!=<>+-]");
		List<String> tableReferences = new ArrayList<String>();

		for (String col : condition) {
			if (!col.equals("")) {
				String tablename = col.split("\\.")[0];
				tableReferences.add(tablename.trim());
			}
		}

		return tableReferences.contains(name);
	}

	/**
	 * Method to convert an expression list into a single expression.
	 *
	 * @param expressionList
	 *            A list containing expressions.
	 * @return A single expression containing all expressions in input list.
	 */
	public Expression listToExpression(List<Expression> expressionList) {
		Expression expressionAccumulator = null;
		for (Expression exp : expressionList) {
			if (expressionAccumulator == null)
				expressionAccumulator = exp;
			else
				expressionAccumulator = new AndExpression(expressionAccumulator, exp);
		}
		return expressionAccumulator;
	}
}
