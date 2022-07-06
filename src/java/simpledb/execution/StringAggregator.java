package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    List<Tuple> tuples;
    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.afield = afield;
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        tuples = new ArrayList<>();
        if(what!=Op.COUNT){
            throw new IllegalArgumentException("what != COUNT");
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        tuples.add(tup);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        if(gbfield == Aggregator.NO_GROUPING) {
            TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(tuples.size()));
            return new SAIterator(tuple);
        }else{
            Map<String,List<Tuple>> map = new HashMap<>();

            for (Tuple tuple : tuples) {
                Field field = tuple.getField(gbfield);
                if(map.get(field.toString())==null){
                    List<Tuple> ls = new ArrayList<>();
                    ls.add(tuple);
                    map.put(field.toString(),ls);
                }else{
                    List<Tuple> tuples = map.get(field.toString());
                    tuples.add(tuple);
                    map.put(field.toString(),tuples);
                }
            }
            List<Tuple> tuples = new ArrayList<>();
            for (List<Tuple> value : map.values()) {
                Tuple aggregate = aggregate(value);
                tuples.add(aggregate);
            }
            return new SAIterator(tuples);
        }


    }

    private Tuple aggregate(List<Tuple> tuples) {
        Field groupField = tuples.get(0).getField(gbfield);
        int count = 0;
        for (Tuple tuple : tuples) {
            count++;
        }
        TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE,gbfieldtype});
        Tuple tuple = new Tuple(td);
        tuple.setField(1, new IntField(count));
        tuple.setField(0,groupField);
        return tuple;
    }

    class SAIterator implements OpIterator{
        List<Tuple> tuples;
        Iterator<Tuple> it;
        SAIterator(Tuple t){
            tuples = new ArrayList<>();
            tuples.add(t);
        }

        SAIterator(List<Tuple> ts){
            tuples = ts;
        }


        @Override
        public void open() throws DbException, TransactionAbortedException {
            it = tuples.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(it == null){
                return false;
            }
            return it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(it == null){
                return null;
            }
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            it = tuples.iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return tuples.get(0).getTupleDesc();
        }

        @Override
        public void close() {
            it = null;
        }
    }

}
