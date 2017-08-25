package DBMS;

/**
 * Class that holds the pageID and tupleID for
 * a given serialized tuple.
 * @author michaelneborak
 *
 */
public class RID {
	 public int pageID = -1;
	 public int tupleID = -1;

	 /**
	  * Constructor for RID.
	  * @param pageID The page number the tuple is on.
	  * @param tupleID The location of this tuple on the given page.
	  */
	public RID(int pageID, int tupleID) {
	    this.pageID = pageID;
		this.tupleID = tupleID;
	}

	@Override
	public String toString() {
		return String.valueOf(pageID)  + "," + String.valueOf(tupleID);
	}
}
