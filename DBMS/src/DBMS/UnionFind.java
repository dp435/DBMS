package DBMS;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import PhysicalOperator.EvaluateExpressionVisitor;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;


public class UnionFind {

	List<UFElement> elements;

	public UnionFind() {
		elements = new ArrayList<UFElement>();
	}
	
	public List<UFElement> getElements() {
		return elements;
	}

	public UFElement find(String attribute) {
		for (UFElement element : elements) {
			for (String attr : element.attributes) {
				if (attribute.equals(attr)) {
					return element;
				}
			}
		}
		UFElement newElement = new UFElement();
		newElement.attributes.add(attribute);
		elements.add(newElement);
		return newElement;
	}

	public void union(UFElement u1, UFElement u2) {
		if (u1 == u2) {
			return;
		}
		UFElement unionedElement = new UFElement();

		unionedElement.upperBound = u1.upperBound;
		unionedElement.setUpperIfValid(u2.upperBound);
		
		unionedElement.lowerBound = u1.lowerBound;
		unionedElement.setLowerIfValid(u2.lowerBound);
		
		unionedElement.equalityConstraint = u1.equalityConstraint;
		unionedElement.setEqualityIfValid(u2.equalityConstraint);

		for (String attr : u1.attributes) {
			unionedElement.attributes.add(attr);
		}

		for (String attr : u2.attributes) {
			if (!unionedElement.attributes.contains(attr)) {
				unionedElement.attributes.add(attr);
			}
		}

		removeElement(u1);
		removeElement(u2);
		elements.add(unionedElement);
	}

	private void removeElement(UFElement u) {
		for (int i = 0; i < elements.size(); i++) {
			if (elements.get(i) == u) {
				elements.remove(i);
				return;
			}
		}
	}

	public static void main(String[] args) throws FileNotFoundException, ParseException {
		
		CCJSqlParser parser = new CCJSqlParser(new FileReader("./benchmark2/sandbox_queries.sql"));
		
		Statement statement;
		Expression whereClause;
		Select select;
		
		if ((statement = parser.Statement()) != null) {
			select = (Select) statement;
			whereClause = ((PlainSelect) select.getSelectBody()).getWhere();
			BuildUnionFindVisitor EV = new BuildUnionFindVisitor();
			whereClause.accept(EV);
			UnionFind buf = EV.getUnionedResult();
			System.out.println("SDF");
		}
		/*
		 * BuildUnionFindVisitor EV = new BuildUnionFindVisitor();
				whereClause.accept(EV);
				if (EV.getResult() == true)
					entryFound = true;
			} else
				break;
		 */

	}

}
