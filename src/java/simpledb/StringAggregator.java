package simpledb;

import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator extends AggregatorBase {

    private static final long serialVersionUID = 1L;

    private Map<Field, Integer> countMap;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        super(gbfield, gbfieldtype, afield, what);
        countMap = new HashMap<>();
    }

    @Override
    protected int getAggInitValue(Op op) {
        return 1;
    }

    @Override
    protected int mergeTupleValue(Op op, int agg, Field gbField, Field aggField) {
        countMap.compute(gbField, (k, v) -> v == null ? 1 : v + 1);
        return countMap.get(gbField);
    }
}
