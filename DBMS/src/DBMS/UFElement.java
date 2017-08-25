package DBMS;

import java.util.ArrayList;
import java.util.List;

public class UFElement {
	public Integer lowerBound;
	public Integer upperBound;
	public Integer equalityConstraint;
	
	public List<String> attributes;
	
	public UFElement(){
		attributes = new ArrayList<>();
		lowerBound = null;
		upperBound = null;
		equalityConstraint = null;
	}
	
	public void setUpperIfValid(Integer i){
		upperBound = getSmallerOrNull(i, upperBound);
	}
	
	public void setLowerIfValid(Integer i){
		lowerBound = getLargerOrNull(i, lowerBound);
	}
	
	public void setEqualityIfValid(Integer i){
		equalityConstraint = getValueOrNull(i, equalityConstraint);
	}
	
	private Integer getValueOrNull(Integer i1, Integer i2) {
		if (i1 == null) {
			return i2;
		}
		return i1;
	}

	private Integer getSmallerOrNull(Integer i1, Integer i2) {
		if (i1 == null) {
			return i2;
		} else if (i2 == null) {
			return i1;
		} else {
			if (i1 < i2) {
				return i1;
			} else {
				return i2;
			}
		}
	}

	private Integer getLargerOrNull(Integer i1, Integer i2) {
		if (i1 == null) {
			return i2;
		} else if (i2 == null) {
			return i1;
		} else {
			if (i1 > i2) {
				return i1;
			} else {
				return i2;
			}
		}
	}

}
