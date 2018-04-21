package simpledb;


import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator extends AggregatorBase {

    private static final long serialVersionUID = 1L;

    static class AVG {
        int count;
        int sum;


        public int avg() {
            return sum / count;
        }
    }

    private Map<Field, AVG> avgMap;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        super(gbfield, gbfieldtype, afield, what);
        avgMap = new HashMap<>();
    }

    @Override
    protected int getAggInitValue(Op op) {
        switch (op) {
            case MIN:
                return Integer.MAX_VALUE;
            case MAX:
                return Integer.MIN_VALUE;
            case AVG:
            case SUM:
                return 0;
            default:
                throw new IllegalStateException("unreachable");
        }
    }

    @Override
    protected int mergeTupleValue(Op op, int agg, Field gbField, Field aggField) {
        final int v = ((IntField) aggField).getValue();

        switch (op) {
            case MIN:
                return Math.min(agg, v);
            case MAX:
                return Math.max(agg, v);
            case AVG:
                avgMap.compute(gbField, (k, avg) -> {
                    if (avg == null) avg = new AVG();
                    avg.count++;
                    avg.sum += v;
                    return avg;
                });
                return avgMap.get(gbField).avg();
            case SUM:
                return agg + v;
            default:
                throw new IllegalStateException("unreachable");
        }
    }
}
