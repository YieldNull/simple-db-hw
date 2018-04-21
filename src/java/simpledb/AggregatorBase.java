package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public abstract class AggregatorBase implements Aggregator {
    private static final long serialVersionUID = 1L;

    private int gbField;
    private Type gbFieldType;
    private int aggField;
    private Op op;

    private Map<Field, Integer> aggResult;

    private TupleDesc tupleDesc;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public AggregatorBase(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggField = afield;
        this.op = what;

        this.aggResult = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (tupleDesc == null) {
            String aggFieldName = String.format("%s(%s)", op.toString(), tup.getTupleDesc().getFieldName(aggField));

            if (gbField >= 0) {
                tupleDesc = new TupleDesc(
                        new Type[]{gbFieldType, Type.INT_TYPE},
                        new String[]{tup.getTupleDesc().getFieldName(gbField), aggFieldName});
            } else {
                tupleDesc = new TupleDesc(
                        new Type[]{Type.INT_TYPE},
                        new String[]{aggFieldName});
            }
        }

        Field field = gbField >= 0 ? tup.getField(gbField) : null;

        aggResult.compute(field,
                (k, agg) -> mergeTupleValue(op,
                        agg == null ? getAggInitValue(op) : agg,
                        field,
                        tup.getField(aggField)));
    }

    protected abstract int getAggInitValue(Op op);

    protected abstract int mergeTupleValue(Op op, int agg, Field gbField, Field aggField);

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        //throw new UnsupportedOperationException("please implement me for lab2");

        return new OpIterator() {
            Iterator<Map.Entry<Field, Integer>> iterator;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                iterator = aggResult.entrySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return iterator.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) throw new NoSuchElementException();

                Map.Entry<Field, Integer> entry = iterator.next();

                Tuple tuple = new Tuple(tupleDesc);
                Field agg = new IntField(entry.getValue());

                if (entry.getKey() == null) {
                    tuple.setField(0, agg);
                } else {
                    tuple.setField(0, entry.getKey());
                    tuple.setField(1, agg);
                }

                return tuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                iterator = aggResult.entrySet().iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return tupleDesc;
            }

            @Override
            public void close() {
                iterator = null;
            }
        };
    }
}
