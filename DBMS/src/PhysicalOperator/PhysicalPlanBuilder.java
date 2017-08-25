package PhysicalOperator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import DBMS.DatabaseCatalog;
import DBMS.JavaUtils;
import DBMS.TupleReaderBinary;
import LogicalOperator.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

public class PhysicalPlanBuilder implements LogicalPlanVisitor {

	private Stack<Operator> OperatorStack; // stack of operators generated from
											// post-order traversal.

	/**
	 * Constructor for PhysicalPlanBuilder.
	 */
	public PhysicalPlanBuilder() {
		OperatorStack = new Stack<Operator>();
	}

	/**
	 * Method to get the root operator of the query tree.
	 * 
	 * @return the root operator of the query tree.
	 */
	public Operator getResult() {
		return OperatorStack.pop();
	}

	/**
	 * Visit method for one of the leaf nodes (LogicalScanOperator).
	 * 
	 * This method pushes a LogicalScanOperator onto the OperatorStack.
	 * 
	 * @param logicalScanOperator
	 *            the operator to be visited
	 */
	@Override
	public void visit(LogicalScanOperator logicalScanOperator) {
		OperatorStack.push(new ScanOperator(logicalScanOperator.tableName, logicalScanOperator.aliasName));
	}

	/**
	 * Visit method for one of the leaf nodes (LogicalSelectionOperator).
	 * 
	 * This method pushes a LogicalSelectionOperator onto the OperatorStack.
	 * 
	 * @param LogicalSelectionOperator
	 *            the operator to be visited
	 */
	@Override
	public void visit(LogicalSelectionOperator logicalSelectionOperator) {
		DatabaseCatalog catalog = DatabaseCatalog.getInstance();
		String tableName = logicalSelectionOperator.tableName;
		String aliasName = logicalSelectionOperator.aliasName;

		List<String> attributeList = catalog.getAttributeInfo(tableName);
		int tupleSize = attributeList.size();
		int numTuples = catalog.getStatistics(tableName).getCount();
		int pageSize = 4096;

		int numPages = (int) Math.ceil((4.0 * tupleSize * numTuples) / (pageSize - 8.0));

		Double lowestCost = null;
		String indexScanAttribute = null;
		for (String attr : attributeList) {
			if (!catalog.isIndexed(tableName, attr))
				continue;
			boolean isClustered = catalog.checkIfClustered(tableName, attr);
			int numLeaves = catalog.getNumLeaves(tableName, attr);

			double reductionFactor = calculateReductionFactor(tableName, aliasName, attr,
					logicalSelectionOperator.selectionCondition);
			Double currentCost;
			if (isClustered) {
				currentCost = 3 + numPages * reductionFactor;
			} else {
				currentCost = 3 + numLeaves * reductionFactor + numTuples * reductionFactor;
			}
			if (lowestCost == null || lowestCost > currentCost) {
				lowestCost = currentCost;
				indexScanAttribute = attr;
			}
		}

		// CASE 1: either no attributes are indexed, or it's cheaper to do a
		// full system scan.
		if (lowestCost == null || lowestCost > numPages) {
			ScanOperator scanner = new ScanOperator(tableName, aliasName);
			OperatorStack.push(
					new SelectionOperator(tableName, logicalSelectionOperator.selectionCondition, aliasName, scanner));
		} else {
			EvaluateIndexExpressionVisitor IV = new EvaluateIndexExpressionVisitor(tableName, aliasName,
					indexScanAttribute);
			logicalSelectionOperator.selectionCondition.accept(IV);
			Expression indexableExpression = IV.getIndexedExpression();
			Expression nonIndexableExpression = IV.getNonIndexedExpression();
			boolean isClustered = catalog.checkIfClustered(tableName, indexScanAttribute);

			// CASE 2: No expressions can utilize indexes.
			if (indexableExpression == null) {
				ScanOperator scanner = new ScanOperator(tableName, aliasName);
				OperatorStack.push(new SelectionOperator(tableName, logicalSelectionOperator.selectionCondition,
						aliasName, scanner));
			}
			// CASE 3: All expressions can utilize indexes.
			else if (nonIndexableExpression == null) {
				OperatorStack.push(new IndexScanOperator(tableName, aliasName, indexScanAttribute, isClustered,
						IV.getLowkey(), IV.getHighkey()));
			}
			// CASE 4: Only some expressions can utilize indexes.
			else {
				IndexScanOperator scanOperator = new IndexScanOperator(tableName, aliasName, indexScanAttribute,
						isClustered, IV.getLowkey(), IV.getHighkey());
				OperatorStack.push(new SelectionOperator(tableName, nonIndexableExpression, aliasName, scanOperator));
			}

		}

	}

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

		if (indexLow != null && indexHigh != null)
			return (double) ((indexHigh - indexLow + 1.0) / baseTableRange);
		if (indexLow != null && indexHigh == null)
			return (double) ((baseTableMaximum - indexLow + 1.0) / baseTableRange);
		if (indexLow == null && indexHigh != null)
			return (double) ((indexHigh - baseTableMinimum + 1.0) / baseTableRange);
		else
			return 1.0;
	}

	/**
	 * Visit method for LogicalJoinOperator node.
	 * 
	 * Recursively visits its left and right subtree respectively and then,
	 * pushes LogicalJoinOperator onto the OperatorStack.
	 * 
	 * @param logicalJoinOperator
	 *            the operator to be visited
	 */
	@Override
	public void visit(LogicalJoinOperator logicalJoinOperator) {

		// Replace this with ordered list if DP gets finished.
		List<LogicalJoinChild> joinChildList = new ConstructJoinTree(logicalJoinOperator).getJoinOrderList();

		List<Operator> operatorList = new ArrayList<Operator>();
		JavaUtils utils = new JavaUtils();

		for (LogicalJoinChild jc : joinChildList) {
			jc.getOperator().accept(this);
			operatorList.add(OperatorStack.pop());
		}

		Expression joinCondition = logicalJoinOperator.getJoinCondition();
		if (joinCondition != null) {
			ArrayList<String> nodesInTree = new ArrayList<String>();

			EvaluateJoinVisitor JV = new EvaluateJoinVisitor();
			joinCondition.accept(JV);
			List<Expression> joinList = JV.getJoinList();

			if (joinChildList.size() >= 2) {
				String firstRelation = joinChildList.get(0).getARName();
				String secondRelation = joinChildList.get(1).getARName();

				nodesInTree.add(firstRelation);
				nodesInTree.add(secondRelation);

				Expression conditionAccumulator = null;
				if (joinList.size() > 0) {
					for (Iterator<Expression> iterator = joinList.iterator(); iterator.hasNext();) {
						Expression exp = iterator.next();
						if (utils.contains(exp.toString(), firstRelation)
								&& utils.contains(exp.toString(), secondRelation)) {
							if (conditionAccumulator == null) {
								conditionAccumulator = exp;
							} else {
								conditionAccumulator = new AndExpression(conditionAccumulator, exp);
							}
							iterator.remove();
						}
					}
				}
				Operator leftOp = operatorList.get(0);
				Operator rightOp = operatorList.get(1);

				if (conditionAccumulator == null) {
					Operator joinTree = new BNLJoinOperator(conditionAccumulator, leftOp, rightOp, 5);
					OperatorStack.push(joinTree);
				} else {
					CheckEqualityVisitor CEV = new CheckEqualityVisitor();
					conditionAccumulator.accept(CEV);
					boolean useSMJ = CEV.getResult();

					if (!useSMJ) {
						Operator joinTree = new BNLJoinOperator(conditionAccumulator, leftOp, rightOp, 5);
						OperatorStack.push(joinTree);

					} else {
						EvaluateJoinVisitor SMJConditionVisitor = new EvaluateJoinVisitor();
						conditionAccumulator.accept(SMJConditionVisitor);
						ArrayList<Expression> joinConditions = SMJConditionVisitor.getJoinList();

						List<String> leftOrderByList = new ArrayList<String>();
						List<String> rightOrderByList = new ArrayList<String>();

						for (Expression exp : joinConditions) {
							String[] equalityExp = exp.toString().split("[!=<>+-] ");
							if (utils.contains(equalityExp[0], firstRelation)) {
								leftOrderByList.add(equalityExp[0].trim());
								rightOrderByList.add(equalityExp[1].trim());
							} else {
								leftOrderByList.add(equalityExp[1].trim());
								rightOrderByList.add(equalityExp[0].trim());
							}

						}

						Operator sortedLeftOp = new ExternalSortOperator(leftOp, 5, leftOrderByList, null,
								leftOrderByList);
						Operator sortedRightOp = new ExternalSortOperator(rightOp, 5, rightOrderByList, null,
								rightOrderByList);
						Operator joinTree = new SMJOperator(conditionAccumulator, sortedLeftOp, sortedRightOp);
						OperatorStack.push(joinTree);
					}
				}
			}

			if (joinChildList.size() >= 3) {
				for (int idx = 2; idx < joinChildList.size(); idx++) {
					String unvisitedRelation = joinChildList.get(idx).getARName();

					Expression conditionAccumulator = null;
					if (joinList.size() > 0) {
						for (Iterator<Expression> iterator = joinList.iterator(); iterator.hasNext();) {
							Expression exp = iterator.next();
							// Find any expressions involving the current
							// relation and the rest of the relations already in
							// the "tree".
							for (String visitedNode : nodesInTree) {
								if (utils.contains(exp.toString(), unvisitedRelation)
										&& utils.contains(exp.toString(), visitedNode)) {
									if (conditionAccumulator == null)
										conditionAccumulator = exp;
									else
										conditionAccumulator = new AndExpression(conditionAccumulator, exp);

									iterator.remove();
								}
							}
						}
					}

					Operator currentJoinTree = OperatorStack.pop();
					Operator newOp = operatorList.get(idx);

					if (conditionAccumulator == null) {
						Operator updatedJoinTree = new BNLJoinOperator(conditionAccumulator, currentJoinTree, newOp, 5);
						OperatorStack.push(updatedJoinTree);

					} else {
						CheckEqualityVisitor CEV = new CheckEqualityVisitor();
						conditionAccumulator.accept(CEV);
						boolean useSMJ = CEV.getResult();

						if (!useSMJ) {
							Operator updatedJoinTree = new BNLJoinOperator(conditionAccumulator, currentJoinTree, newOp,
									5);
							OperatorStack.push(updatedJoinTree);

						} else {
							EvaluateJoinVisitor SMJConditionVisitor = new EvaluateJoinVisitor();
							conditionAccumulator.accept(SMJConditionVisitor);
							ArrayList<Expression> joinConditions = SMJConditionVisitor.getJoinList();

							List<String> leftOrderByList = new ArrayList<String>();
							List<String> rightOrderByList = new ArrayList<String>();

							for (Expression exp : joinConditions) {
								String[] equalityExp = exp.toString().split("[!=<>+-] ");
								if (!utils.contains(equalityExp[0], unvisitedRelation)) {
									leftOrderByList.add(equalityExp[0].trim());
									rightOrderByList.add(equalityExp[1].trim());
								} else {
									leftOrderByList.add(equalityExp[1].trim());
									rightOrderByList.add(equalityExp[0].trim());
								}
							}

							Operator sortedJoinOp = new ExternalSortOperator(currentJoinTree, 5, leftOrderByList, null,
									leftOrderByList);
							Operator sortedRightOp = new ExternalSortOperator(newOp, 5, rightOrderByList, null,
									rightOrderByList);
							Operator joinTree = new SMJOperator(conditionAccumulator, sortedJoinOp, sortedRightOp);
							OperatorStack.push(joinTree);
						}

					}
				}

			}
		} else {
			if (joinChildList.size() >= 2) {
				Operator leftOp = operatorList.get(0);
				Operator rightOp = operatorList.get(1);

				Operator joinTree = new BNLJoinOperator(null, leftOp, rightOp,5 );
				OperatorStack.push(joinTree);

			}
			// If there exists more than one join, build onto the previously
			// constructed JoinOperator with the next tables in queue in a
			// bottom-to-top and left-heavy manner.
			if (joinChildList.size() >= 3) {
				for (int idx = 2; idx < joinChildList.size(); idx++) {
					Operator currentJoinTree = OperatorStack.pop();
					Operator newOp = operatorList.get(idx);
					Operator updatedJoinTree = new BNLJoinOperator(null, currentJoinTree, newOp, 5);
					OperatorStack.push(updatedJoinTree);
				}
			}
		}

	}

	/**
	 * Visit method for LogicalProjectionOperator node.
	 * 
	 * Recursively visits its subtree and then, pushes LogicalProjectionOperator
	 * onto the OperatorStack.
	 * 
	 * @param logicalProjectionOperator
	 *            the operator to be visited
	 */
	@Override
	public void visit(LogicalProjectionOperator logicalProjectionOperator) {
		logicalProjectionOperator.childOperator.accept(this);
		Operator childOperator = OperatorStack.pop();
		OperatorStack.push(new ProjectionOperator(logicalProjectionOperator.projectionCondition, childOperator));
	}

	/**
	 * Visit method for LogicalSortOperator node.
	 * 
	 * Recursively visits its subtree and then, pushes LogicalSortOperator onto
	 * the OperatorStack.
	 * 
	 * @param logicalSortOperator
	 *            the operator to be visited
	 */
	@Override
	public void visit(LogicalSortOperator logicalSortOperator) {
		logicalSortOperator.childOperator.accept(this);
		Operator childOperator = OperatorStack.pop();
		String sortMethod = logicalSortOperator.getSortMethod();
		int sortBuffer = logicalSortOperator.getBufferCount();
		if (sortMethod.equals("INTERNAL")) {
			OperatorStack.push(new SortOperator(childOperator, logicalSortOperator.sortOrder,
					logicalSortOperator.outputOrderList, logicalSortOperator.printOrder));
		} else {
			OperatorStack.push(new ExternalSortOperator(childOperator, sortBuffer, logicalSortOperator.sortOrder,
					logicalSortOperator.outputOrderList, logicalSortOperator.printOrder));
		}
	}

	/**
	 * Visit method for LogicalDuplicateEliminationOperator node.
	 * 
	 * Recursively visits its subtree and then, pushes
	 * LogicalDuplicateEliminationOperator onto the OperatorStack.
	 * 
	 * @param logicalDuplicateEliminationOperator
	 *            the operator to be visited
	 */
	@Override
	public void visit(LogicalDuplicateEliminationOperator logicalDuplicateEliminationOperator) {
		logicalDuplicateEliminationOperator.childOperator.accept(this);
		Operator childOperator = OperatorStack.pop();
		OperatorStack.push(new DuplicateEliminationOperator(childOperator));
	}

	/**
	 * Visit method for LogicalIndexScanOperator node.
	 * 
	 * @param LogicalIndexScanOperator
	 *            the operator to be visited
	 */
	@Override
	public void visit(LogicalIndexScanOperator logicalIndexScanOperator) {
		OperatorStack.push(new IndexScanOperator(logicalIndexScanOperator.tableName, logicalIndexScanOperator.aliasName,
				logicalIndexScanOperator.indexedAttribute, logicalIndexScanOperator.isClustered,
				logicalIndexScanOperator.lowerBound, logicalIndexScanOperator.upperBound));

	}

}
