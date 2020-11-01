package edu.coursera.parallel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

/**
 * Class wrapping methods for implementing reciprocal array sum in parallel.
 */
public final class ReciprocalArraySum {

	/**
	 * Default constructor.
	 */

	private ReciprocalArraySum() {
	}

	/**
	 * Sequentially compute the sum of the reciprocal values for a given array.
	 *
	 * @param input Input array
	 * @return The sum of the reciprocals of the array input
	 */
	protected static double seqArraySum(final double[] input) {
		double sum = 0;

		// Compute sum of reciprocals of array elements
		for (int i = 0; i < input.length; i++) {
			sum += 1 / input[i];
		}

		return sum;
	}

	/**
	 * Computes the size of each chunk, given the number of chunks to create across
	 * a given number of elements.
	 *
	 * @param nChunks   The number of chunks to create
	 * @param nElements The number of elements to chunk across
	 * @return The default chunk size
	 */
	private static int getChunkSize(final int nChunks, final int nElements) {
		// Integer ceil
		return (nElements + nChunks - 1) / nChunks;
	}

	/**
	 * Computes the inclusive element index that the provided chunk starts at, given
	 * there are a certain number of chunks.
	 *
	 * @param chunk     The chunk to compute the start of
	 * @param nChunks   The number of chunks created
	 * @param nElements The number of elements to chunk across
	 * @return The inclusive index that this chunk starts at in the set of nElements
	 */
	private static int getChunkStartInclusive(final int chunk, final int nChunks, final int nElements) {
		final int chunkSize = getChunkSize(nChunks, nElements);
		return chunk * chunkSize;
	}

	/**
	 * Computes the exclusive element index that the provided chunk ends at, given
	 * there are a certain number of chunks.
	 *
	 * @param chunk     The chunk to compute the end of
	 * @param nChunks   The number of chunks created
	 * @param nElements The number of elements to chunk across
	 * @return The exclusive end index for this chunk
	 */
	private static int getChunkEndExclusive(final int chunk, final int nChunks, final int nElements) {
		final int chunkSize = getChunkSize(nChunks, nElements);
		final int end = (chunk + 1) * chunkSize;
		if (end > nElements) {
			return nElements;
		} else {
			return end;
		}
	}

	/**
	 * This class stub can be filled in to implement the body of each task created
	 * to perform reciprocal array sum in parallel.
	 */
	private static class ReciprocalArraySumTask extends RecursiveAction {
		/**
		 * Starting index for traversal done by this task.
		 */

		private final int startIndexInclusive;
		/**
		 * Ending index for traversal done by this task.
		 */
		private final int endIndexExclusive;
		/**
		 * Input array to reciprocal sum.
		 */
		private final double[] input;
		/**
		 * Intermediate value produced by this task.
		 */
		private double value;

		private int seq_threshold = 2000;

		private int chunks;

		/**
		 * Constructor.
		 * 
		 * @param setStartIndexInclusive Set the starting index to begin parallel
		 *                               traversal at.
		 * @param setEndIndexExclusive   Set ending index for parallel traversal.
		 * @param setInput               Input values
		 */
		ReciprocalArraySumTask(final int setStartIndexInclusive, final int setEndIndexExclusive,
				final double[] setInput, int chunks) {
			this.startIndexInclusive = setStartIndexInclusive;
			this.endIndexExclusive = setEndIndexExclusive;
			this.input = setInput;
			this.chunks = chunks;
			if (chunks > 2)
				this.seq_threshold = 75000;
		}

		/**
		 * Getter for the value produced by this task.
		 * 
		 * @return Value produced by this task
		 */
		public double getValue() {
			return value;
		}

		// @Override
		/*
		 * protected void compute() { // TODO int length = (endIndexExclusive -
		 * startIndexInclusive);
		 * 
		 * double summation = 0;
		 * 
		 * if ((length) <= seq_threshold) {
		 * 
		 * for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
		 * 
		 * summation += (1 / input[i]);
		 * 
		 * }
		 * 
		 * } else { List<ReciprocalArraySumTask> rct = new ArrayList<>(); This is the
		 * code for recursive divide and conquer 2 parter ReciprocalArraySumTask left =
		 * new ReciprocalArraySumTask(startIndexInclusive, (startIndexInclusive +
		 * endIndexExclusive) / 2, input, 2); ReciprocalArraySumTask right = new
		 * ReciprocalArraySumTask((startIndexInclusive + endIndexExclusive) / 2,
		 * endIndexExclusive, input, 2);
		 * 
		 * left.fork(); right.compute(); left.join(); summation = left.getValue() +
		 * right.getValue(); }
		 * 
		 * 
		 * 
		 * 
		 * this is the code for recursive divde and conquer for n chunks for (int i = 0;
		 * i < chunks; i++) {
		 * 
		 * int start = getChunkStartInclusive(i, chunks, length); int end =
		 * getChunkEndExclusive(i, chunks, length);
		 * 
		 * ReciprocalArraySumTask t = new ReciprocalArraySumTask((startIndexInclusive +
		 * start), (startIndexInclusive + end), input, chunks);
		 * 
		 * rct.add(t); if (i > 0) t.fork(); }
		 * 
		 * for (int i = 1; i < chunks; i++) {
		 * 
		 * rct.get(i).fork();
		 * 
		 * }
		 * 
		 * 
		 * rct.get(0).compute();
		 * 
		 * for (int i = 1; i < chunks; i++) {
		 * 
		 * rct.get(i).join(); summation += rct.get(i).getValue(); }
		 * 
		 * summation += rct.get(0).getValue();
		 * 
		 * 
		 * // this is some random code for (ReciprocalArraySumTask k : rct) { summation
		 * += k.getValue(); }
		 * 
		 * }
		 * 
		 * 
		 * 
		 * //Writing the code for flat chunked divide and conquer
		 * 
		 */

		@Override
		public void compute() {
			double summation = 0;
			for (int i = startIndexInclusive; i < endIndexExclusive; i++) {

				summation += (1 / input[i]);

			}

			this.value = summation;
		}

	}

	/**
	 * TODO: Modify this method to compute the same reciprocal sum as seqArraySum,
	 * but use two tasks running in parallel under the Java Fork Join framework. You
	 * may assume that the length of the input array is evenly divisible by 2.
	 *
	 * @param input Input array
	 * @return The sum of the reciprocals of the array input
	 */
	protected static double parArraySum(final double[] input) {
		assert input.length % 2 == 0;

		double sum = 0;

		// Compute sum of reciprocals of array elements
//		for (int i = 0; i < input.length; i++) {
//			sum += 1 / input[i];
//		}

		// ReciprocalArraySumTask task = new ReciprocalArraySumTask(0, input.length,
		// input, input.length/2);

		// fkp.invoke(task);

		// sum = task.getValue();
		sum = parManyTaskArraySum(input, 2);
		return sum;
	}

	/**
	 * TODO: Extend the work you did to implement parArraySum to use a set number of
	 * tasks to compute the reciprocal array sum. You may find the above utilities
	 * getChunkStartInclusive and getChunkEndExclusive helpful in computing the
	 * range of element indices that belong to each chunk.
	 *
	 * @param input    Input array
	 * @param numTasks The number of tasks to create
	 * @return The sum of the reciprocals of the array input
	 */
	protected static double parManyTaskArraySum(final double[] input, final int numTasks) {
		double sum = 0;

		// Compute sum of reciprocals of array elements
//		for (int i = 0; i < input.length; i++) {
//			sum += 1 / input[i];
//		}
		ForkJoinPool fkp = new ForkJoinPool(4);

		List<ReciprocalArraySumTask> rct = new ArrayList<>();

		for (int i = 0; i < numTasks; i++) {
			int start = getChunkStartInclusive(i, numTasks, input.length);
			int end = getChunkEndExclusive(i, numTasks, input.length);

			ReciprocalArraySumTask t = new ReciprocalArraySumTask(start, end, input, numTasks);
			rct.add(t);
		}

		for (int i = 1; i < numTasks; i++) {
			rct.get(i).fork();
		}

		rct.get(0).invoke();

		for (int i = 1; i < numTasks; i++) {
			rct.get(i).join();
			sum += rct.get(i).getValue();
		}

		sum += rct.get(0).getValue();

		return sum;
	}
}
