package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

import static simpledb.execution.Aggregator.Op.MAX;
import static simpledb.execution.Aggregator.Op.MIN;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    List<Tuple> tuples;
    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.afield = afield;
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        tuples = new ArrayList<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        tuples.add(tup);

    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        if(gbfield == Aggregator.NO_GROUPING){

            if(what==MIN) {
                int min = Integer.parseInt(tuples.get(0).getField(afield).toString());
                for (Tuple tuple : tuples) {
                    min = Math.min(min, Integer.parseInt(tuple.getField(afield).toString()));
                }
                TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
                Tuple tuple = new Tuple(td);
                tuple.setField(0, new IntField(min));
                return new IAIterator(tuple);
            }else if(what == MAX) {
                int max = Integer.parseInt(tuples.get(0).getField(afield).toString());
                for (Tuple tuple : tuples) {
                    max = Math.max(max, Integer.parseInt(tuple.getField(afield).toString()));
                }
                TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
                Tuple tuple = new Tuple(td);
                tuple.setField(0, new IntField(max));
                return new IAIterator(tuple);
            }else if(what == Op.AVG){
                int sum = 0;
                int count = 0;
                for (Tuple tuple : tuples) {
                    count++;
                    sum += Integer.parseInt(tuple.getField(afield).toString());
                }
                TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
                Tuple tuple = new Tuple(td);
                tuple.setField(0, new IntField(sum/count));
                return new IAIterator(tuple);
            }else if(what == Op.COUNT){

                int count = 0;
                for (Tuple tuple : tuples) {
                    count++;
                }
                TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
                Tuple tuple = new Tuple(td);
                tuple.setField(0, new IntField(count));
                return new IAIterator(tuple);
            }else if(what == Op.SUM){
                int sum = 0;

                for (Tuple tuple : tuples) {

                    sum += Integer.parseInt(tuple.getField(afield).toString());
                }
                TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
                Tuple tuple = new Tuple(td);
                tuple.setField(0, new IntField(sum));
                return new IAIterator(tuple);
            }else{
                System.out.println("不支持的运算符");
                return null;
            }
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
            return new IAIterator(tuples);
        }
    }
    public Tuple aggregate(List<Tuple> tuples){
        Field groupField = tuples.get(0).getField(gbfield);
        if(what==MIN) {
            int min = Integer.parseInt(tuples.get(0).getField(afield).toString());
            for (Tuple tuple : tuples) {
                min = Math.min(min, Integer.parseInt(tuple.getField(afield).toString()));
            }
            TupleDesc td = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE});
            Tuple tuple = new Tuple(td);
            tuple.setField(1, new IntField(min));
            tuple.setField(0,groupField);
            return tuple;
        }else if(what == MAX) {
            int max = Integer.parseInt(tuples.get(0).getField(afield).toString());
            for (Tuple tuple : tuples) {
                max = Math.max(max, Integer.parseInt(tuple.getField(afield).toString()));
            }
            TupleDesc td = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE});
            Tuple tuple = new Tuple(td);
            tuple.setField(1, new IntField(max));
            tuple.setField(0,groupField);
            return tuple;
        }else if(what == Op.AVG){
            int sum = 0;
            int count = 0;
            for (Tuple tuple : tuples) {
                count++;
                sum += Integer.parseInt(tuple.getField(afield).toString());
            }
            TupleDesc td = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE});
            Tuple tuple = new Tuple(td);
            tuple.setField(1, new IntField(sum/count));
            tuple.setField(0,groupField);
            return tuple;
        }else if(what == Op.COUNT){

            int count = 0;
            for (Tuple tuple : tuples) {
                count++;
            }
            TupleDesc td = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE});
            Tuple tuple = new Tuple(td);
            tuple.setField(1, new IntField(count));
            tuple.setField(0,groupField);
            return tuple;
        }else if(what == Op.SUM){
            int sum = 0;

            for (Tuple tuple : tuples) {

                sum += Integer.parseInt(tuple.getField(afield).toString());
            }
            TupleDesc td = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE});
            Tuple tuple = new Tuple(td);
            tuple.setField(1, new IntField(sum));
            tuple.setField(0,groupField);
            return tuple;
        }else{
            System.out.println("不支持的运算符");
            return null;
        }
    }

    class IAIterator implements OpIterator{
        List<Tuple> tuples;
        Iterator<Tuple> it;
        IAIterator(Tuple t){
            tuples = new ArrayList<>();
            tuples.add(t);
        }

        IAIterator(List<Tuple> ts){
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
