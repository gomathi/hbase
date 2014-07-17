package org.apache.hadoop.hbase.master.rrlbalancer;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * Returns the {@link ServerAndLoadForTest} which have a load less than the given
 * filtersize.
 * 
 */
public class ServersByLoadIterator implements Iterator<ServerAndLoad> {

	private final Iterator<ServerAndLoad> intItr;
	private final int filterSize;
	private final Queue<ServerAndLoad> processingQue;

	public ServersByLoadIterator(final Iterator<ServerAndLoad> intItr,
			final int filterSize) {
		this.intItr = intItr;
		this.filterSize = filterSize;
		processingQue = new ArrayDeque<ServerAndLoad>();
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if (processingQue.isEmpty())
			addElement();
		return processingQue.size() > 0;
	}

	@Override
	public ServerAndLoad next() {
		// TODO Auto-generated method stub
		if (processingQue.size() > 0)
			return processingQue.remove();
		throw new NoSuchElementException(
				"No elements available to be returned.");
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Operation is not supported.");
	}

	private void addElement() {
		while (intItr.hasNext()) {
			ServerAndLoad temp = intItr.next();
			if (temp.getLoad() < filterSize) {
				processingQue.add(temp);
				break;
			}
		}
	}

}
