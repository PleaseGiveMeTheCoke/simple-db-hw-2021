package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;


    OpIterator child;
    int afield;
    int gfield;
    Aggregator.Op aop;
    List<Tuple> children;
    Iterator<Tuple> aggRes;
    List<Tuple> il;
    List<Tuple> sl;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop)  {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.aop = aop;
        this.gfield = gfield;
        this.il = new ArrayList<>();
        this.sl = new ArrayList<>();
        children = new ArrayList<>();

        try {
            child.open();
            while (child.hasNext()){
                children.add(child.next());
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        if(gfield == Aggregator.NO_GROUPING){
            return null;
        }else{
            return child.getTupleDesc().getFieldName(gfield);
        }
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        if(child.getTupleDesc().getFieldType(afield).equals(Type.INT_TYPE)){
            IntegerAggregator ia=new IntegerAggregator(gfield, child.getTupleDesc().getFieldType(gfield), afield, aop);
            for (Tuple tuple : children) {
                ia.mergeTupleIntoGroup(tuple);
            }
            OpIterator iterator = ia.iterator();
            iterator.open();
            while (iterator.hasNext()){
                il.add(iterator.next());
            }
            aggRes = il.iterator();
        }else{
           StringAggregator sa = new StringAggregator(gfield, child.getTupleDesc().getFieldType(gfield), afield, aop);
            for (Tuple tuple : children) {
                sa.mergeTupleIntoGroup(tuple);
            }
            OpIterator iterator = sa.iterator();
            iterator.open();
            while (iterator.hasNext()){
                sl.add(iterator.next());
            }
            aggRes = sl.iterator();
        }
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(aggRes == null){
            return null;
        }
        if(aggRes.hasNext()){
            return aggRes.next();
        }else{
            return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        if(child.getTupleDesc().getFieldType(afield).equals(Type.INT_TYPE)){
            aggRes = il.iterator();
        }else{
            aggRes = sl.iterator();
        }
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        if(gfield == Aggregator.NO_GROUPING){
            return new TupleDesc(new Type[]{child.getTupleDesc().getFieldType(afield)},new String[]{nameOfAggregatorOp(aop)+" ("+child.getTupleDesc().getFieldName(afield)+")"});
        }else{
            return new TupleDesc(new Type[]{child.getTupleDesc().getFieldType(gfield),child.getTupleDesc().getFieldType(afield)}
            ,new String[]{child.getTupleDesc().getFieldName(gfield),nameOfAggregatorOp(aop)+" ("+child.getTupleDesc().getFieldName(afield)+")"});
        }
    }

    public void close() {
        aggRes = null;
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
