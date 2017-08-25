package PhysicalOperator;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import DBMS.DatabaseCatalog;
import DBMS.RecordComparator;
import DBMS.Tuple;
import DBMS.TupleConverter;
import DBMS.TupleReaderBinary;
import DBMS.TupleWriterBinary;

/**
 * Class to sort tuples given a child operator. Does so with a bounded in-memory
 * buffer.
 * 
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 *
 */
public class ExternalSortOperator extends Operator {

	private int numInputBuffers;
	private List<String> sortOrder;
	public static final int BUFFER_SIZE = TupleReaderBinary.BUFFER_SIZE;
	public static final int METADATA_SIZE = 8;

	private int prevTotalBuckets = 0;
	private int prevPrevTotalBuckets = 0;
	private int passNumber;
	public Operator childOperator;

	private int bufferCapacity;

	private boolean sorted;
	private TupleReaderBinary sortedReader;

	private List<String> outputOrderList;
	private List<String> ordering;
	public List<String> printOrder;
	RecordComparator theComparator;

	private List<Tuple> outputBuffer;
	TupleWriterBinary outputWriter;
	int instanceHashcode;
	int writerBucketID = 0;
	int readerBucketID = 0;
	int writerFlushes = 0;
	boolean invalid = false;
	int currentGroup = 1;
	Tuple childTuple;

	/**
	 * Constructor for ExternalSortOperator.
	 * 
	 * @param childOperator
	 *            The child operator.
	 * @param numBuffers
	 *            number of buffers allowed for sorting.
	 * @param sortOperators
	 *            specified sort order.
	 */
	public ExternalSortOperator(Operator childOperator, int numBuffers, List<String> orderByList,
			List<String> outputOrder, List<String> printOrder) {
		numInputBuffers = numBuffers - 1;
		this.printOrder = printOrder;
		if (outputOrder != null) {
			outputOrderList = new ArrayList<String>(outputOrder);
			sortOrder = new ArrayList<String>();
			
			for (Object item : orderByList) {
				sortOrder.add(item.toString());
				outputOrderList.remove(item.toString());
			}
			sortOrder.addAll(outputOrderList);
		} else {
			sortOrder = new ArrayList<String>();
			sortOrder.addAll(orderByList);
		}

		passNumber = 0;
		this.childOperator = childOperator;
		childTuple = childOperator.getNextTuple();
		outputBuffer = new ArrayList<Tuple>();

		if (childTuple != null) {
			bufferCapacity = (TupleReaderBinary.BUFFER_SIZE / (4 * childTuple.size()));
			outputBuffer.add(childTuple);
			ordering = childTuple.ordering;
		} else {
			invalid = true;
			bufferCapacity = 0;
		}

		sorted = false;
		sortedReader = null;
		instanceHashcode = this.hashCode() + randomHelper();
		String[] comparisons = sortOrder.toArray(new String[0]);
		theComparator = new RecordComparator(comparisons);
	}

	private void sortOutputBuffer() {
		outputBuffer.sort(theComparator);
	}

	private void flushOutputBuffer() {
		while (!outputBuffer.isEmpty()) {
			outputWriter.writeTuple(outputBuffer.remove(0).convertToFOT());
		}
		outputWriter.flush();
	}

	/**
	 * Run pass 0 of the ES algorithm.
	 */
	private void runPassZero() {
		writerBucketID = 0;
		passNumber = 0;
		outputWriter = new TupleWriterBinary(DatabaseCatalog.getInstance().getTempDirectory() + "/" + instanceHashcode
				+ "_" + passNumber + "_" + writerBucketID);

		while (true) {
			while (outputBuffer.size() < bufferCapacity) {
				Tuple temp = childOperator.getNextTuple();
				if (temp != null) {
					outputBuffer.add(temp);
				} else {
					sortOutputBuffer();
					flushOutputBuffer();
					outputWriter.close();
					prevTotalBuckets = writerBucketID + 1;
					writerBucketID = 0;
					passNumber = 1;
					return;
				}
			}
			sortOutputBuffer();
			flushOutputBuffer();
			writerBucketID++;
			outputWriter = new TupleWriterBinary(DatabaseCatalog.getInstance().getTempDirectory() + "/"
					+ instanceHashcode + "_" + passNumber + "_" + writerBucketID);

		}
	}

	/**
	 * Run the main sort pass (passes 1-N) of the ES algorithm
	 */
	private void sort() {
		runPassZero();
		List<TupleReaderBinary> buffers = new ArrayList<>();
		List<Tuple> sortStage = new ArrayList<>();
		assert (readerBucketID == 0);
		assert (writerBucketID == 0);
		assert (passNumber == 1);
		assert (currentGroup == 1);
		outputBuffer.clear();

		// assign buffers
		for (int i = 0; i < numInputBuffers; i++) {
			buffers.add(null);
			assignBuffer(buffers, i);
			sortStage.add(null);
		}

		assert (buffers.size() == numInputBuffers);
		assert (sortStage.size() == numInputBuffers);

		// read from buffers into list of size numInputBuffers
		for (int i = 0; i < numInputBuffers; i++) {
			sortStage.set(i, readFromBuffer(buffers, i));
		}

		outputWriter = new TupleWriterBinary(DatabaseCatalog.getInstance().getTempDirectory() + "/" + instanceHashcode
				+ "_" + passNumber + "_" + writerBucketID);

		while (true) {
			if (!findAndReplaceSmallest(buffers, sortStage)) {
				prevPrevTotalBuckets = prevTotalBuckets;
				prevTotalBuckets = writerBucketID + 1;
				flushOutputBuffer();
				outputWriter.close();

				// We are done! Return
				if (prevTotalBuckets == 1) {
					deleteTempFiles();
					return;
				}
				writerBucketID = 0;
				readerBucketID = 0;
				writerFlushes = 0;
				deleteTempFiles();
				passNumber++;
				currentGroup = 1;

				for (int i = 0; i < numInputBuffers; i++) {
					assignBuffer(buffers, i);
					sortStage.add(null);
				}
				for (int i = 0; i < numInputBuffers; i++) {
					sortStage.set(i, readFromBuffer(buffers, i));
				}

				outputWriter = new TupleWriterBinary(DatabaseCatalog.getInstance().getTempDirectory() + "/"
						+ instanceHashcode + "_" + passNumber + "_" + writerBucketID);
			}
		}

	}

	/**
	 * Find smallest tuple; replace tuple from buffer at that position
	 * 
	 * @param buffers
	 *            Buffers to query for next tuple.
	 * @param sortStage
	 *            List of tuples to query for smallest tuple.
	 * @return True if more tuples in buffers, false otherwise
	 */
	private boolean findAndReplaceSmallest(List<TupleReaderBinary> buffers, List<Tuple> sortStage) {
		// assert(buffers.size() == sortStage.size()) && buffers.size() ==
		// numInputBuffers);
		Tuple smallest = null;
		int smallestIndex = 0;
		for (int i = 0; i < numInputBuffers; i++) {
			if (theComparator.compare(smallest, sortStage.get(i)) > 0) {
				smallest = sortStage.get(i);
				smallestIndex = i;
			}
		}
		if (smallest != null) {
			placeInOutput(smallest);
		} else {
			if (readerBucketID < prevTotalBuckets) {
				currentGroup++;
				for (int i = 0; i < numInputBuffers; i++) {
					assignBuffer(buffers, i);
					sortStage.add(null);
				}
				for (int i = 0; i < numInputBuffers; i++) {
					sortStage.set(i, readFromBuffer(buffers, i));
				}
				return true;
			} else {
				// kill switch - NO MORE TUPLES TO SORT!
				return false;
			}
		}
		sortStage.set(smallestIndex, readFromBuffer(buffers, smallestIndex));
		return true;
	}

	/**
	 * Places the given tuple into the output buffer.
	 * 
	 * @param tuple
	 *            Tuple to be put in the output buffer
	 */
	private void placeInOutput(Tuple tuple) {
		if (outputBuffer.size() < bufferCapacity) {
			outputBuffer.add(tuple);
		} else {
			flushOutputBuffer();
			if (outputBuffer.size() < bufferCapacity) {
				outputBuffer.add(tuple);
			} else {
				System.out.println("MAJOR ERROR IN WRITING TO ES OUTPUT BUFFER");
			}
			writerFlushes++;
			// next bucket when we have written numInputBuffers^passNumber
			if (writerFlushes % Math.pow(numInputBuffers, passNumber) == 0) {
				writerBucketID++;
				outputWriter = new TupleWriterBinary(DatabaseCatalog.getInstance().getTempDirectory() + "/"
						+ instanceHashcode + "_" + passNumber + "_" + writerBucketID);
			}
		}
	}

	/**
	 * Assign a new TupleReaderBinary to the given buffer.
	 * 
	 * @param buffers
	 *            The list of TupleReaderBinary to assign to
	 * @param bufferID
	 *            The specific buffer in buffers to assign
	 */
	// reassign buffer x
	private void assignBuffer(List<TupleReaderBinary> buffers, int bufferID) {
		if (readerBucketID < prevTotalBuckets && readerBucketID < numInputBuffers * currentGroup) {
			TupleReaderBinary readerBinary = new TupleReaderBinary(DatabaseCatalog.getInstance().getTempDirectory()
					+ "/" + instanceHashcode + "_" + (passNumber - 1) + "_" + readerBucketID);
			readerBinary.keyNames = ordering;
			buffers.set(bufferID, readerBinary);
			readerBucketID++;
		} else {
			buffers.set(bufferID, null);
		}
	}

	/**
	 * Read from given buffer; if buffer is empty, try reading next
	 * 
	 * @param buffers
	 *            The list of TupleReaderBinary to read from
	 * @param bufferID
	 *            The specific buffer in buffers to read
	 * @return If no next tuple in bufferID exists, return null, otherwise
	 *         return next tuple
	 */
	private Tuple readFromBuffer(List<TupleReaderBinary> buffers, int bufferID) {
		if (buffers.get(bufferID) == null) {
			return null;
		}
		Tuple temp = buffers.get(bufferID).readTuple(true);

		if (temp != null) {
			temp.ordering = ordering;
			return temp;
		} else {
			assignBuffer(buffers, bufferID);
			if (buffers.get(bufferID) == null) {
				return null;
			}
			temp = buffers.get(bufferID).readTuple(true);

			if (temp != null) {
				temp.ordering = ordering;
				return temp;
			} else {
				return null;
			}
		}
	}

	/**
	 * @return The next tuple from the sorted input.
	 */
	@Override
	public Tuple getNextTuple() {
		if (invalid) {
			return null;
		}
		if (sorted == false) {
			this.sort();
			String tempDirectory = DatabaseCatalog.getInstance().getTempDirectory();
			String filepath = tempDirectory + "/" + instanceHashcode + "_" + (passNumber) + "_" + 0;
			TupleConverter tc = new TupleConverter();
			tc.compress(filepath);
			sortedReader = new TupleReaderBinary(filepath);
			sortedReader.keyNames = ordering;
			sorted = true;
		}
		Tuple ret = sortedReader.readTuple(true);
		if (ret == null) {
			/*
			 * try { File file = new
			 * File(DatabaseCatalog.getInstance().getTempDirectory() + "/" +
			 * instanceHashcode + "_" + (passNumber) + "_" + 0); file.delete();
			 * } catch (Exception e) { e.printStackTrace(); }
			 */
		} else {
			ret.ordering = ordering;
		}
		return ret;
	}

	@Override
	public void reset() {
		sorted = false;
	}

	/**
	 * Reset this operator to its initial state.
	 */
	@Override
	public void reset(int index) {
		if (sorted) {
			sortedReader.reset(index);
		}
	}

	/**
	 * Generate a random number between 1 and 10000.
	 * 
	 * @return A random number between 1 and 10000.
	 */
	private int randomHelper() {
		Random rand = new Random();
		int randomNum = rand.nextInt((10000 - 1) + 1) + 1;
		return randomNum;
	}

	/**
	 * Remove temp files used by the ES algorithm.
	 */
	private void deleteTempFiles() {
		for (int i = 0; i < prevPrevTotalBuckets; i++) {
			try {
				String filename = DatabaseCatalog.getInstance().getTempDirectory() + "/" + instanceHashcode + "_"
						+ (passNumber - 1) + "_" + i;
				File file = new File(filename);
				file.delete();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
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
