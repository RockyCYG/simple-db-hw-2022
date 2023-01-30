package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;

    private final Type gbfieldtype;

    private final int afield;

    private final Op what;

    private Map<Field, Integer> resultMap;

    private Map<Field, Integer> countMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.resultMap = new HashMap<>();
        this.countMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        int newValue = ((IntField) tup.getField(afield)).getValue();
        Field field = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        countMap.merge(field, 1, Integer::sum);
        switch (what) {
            case MIN -> resultMap.merge(field, newValue, Math::min);
            case MAX -> resultMap.merge(field, newValue, Math::max);
            case SUM, AVG -> resultMap.merge(field, newValue, Integer::sum);
            case COUNT -> resultMap.merge(field, 1, Integer::sum);
            case SUM_COUNT -> {
            }
            case SC_AVG -> {
            }
            default -> {
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        return new IntegerAggregatorIterator();
    }

    private class IntegerAggregatorIterator implements OpIterator {

        private TupleIterator iterator;

        private TupleDesc tupleDesc;

        public IntegerAggregatorIterator() {
            if (gbfield == NO_GROUPING) {
                this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
                List<Tuple> tuples = new ArrayList<>();
                for (Map.Entry<Field, Integer> entry : resultMap.entrySet()) {
                    Tuple tuple = new Tuple(tupleDesc);
                    Integer value = entry.getValue();
                    if (what == Op.AVG) {
                        value /= countMap.get(entry.getKey());
                    }
                    tuple.setField(0, new IntField(value));
                    tuples.add(tuple);
                }
                this.iterator = new TupleIterator(tupleDesc, tuples);
            } else {
                this.tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
                List<Tuple> tuples = new ArrayList<>();
                for (Map.Entry<Field, Integer> entry : resultMap.entrySet()) {
                    Tuple tuple = new Tuple(tupleDesc);
                    Integer value = entry.getValue();
                    if (what == Op.AVG) {
                        value /= countMap.get(entry.getKey());
                    }
                    tuple.setField(0, entry.getKey());
                    tuple.setField(1, new IntField(value));
                    tuples.add(tuple);
                }
                this.iterator = new TupleIterator(tupleDesc, tuples);
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            iterator.open();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            iterator.rewind();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return tupleDesc;
        }

        @Override
        public void close() {
            iterator.close();
        }
    }

}
