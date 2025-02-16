package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate p;
    private OpIterator child1;
    private OpIterator child2;

    private final List<Tuple> child1Tups = new ArrayList<>();
    private final List<Tuple> child2Tups = new ArrayList<>();
    private final List<Tuple> tups = new ArrayList<>();

    Iterator<Tuple> child1It;
    Iterator<Tuple> child2It;
    Iterator<Tuple> it;
    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;

    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return child2.getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc td1 = child1.getTupleDesc();
        TupleDesc td2 = child2.getTupleDesc();
        int nums = td1.numFields()+td2.numFields();
        Type[] ts = new Type[nums];
        String[] fs = new String[nums];
        int i = 0;
        for (int j = 0; j < td1.numFields(); j++,i++) {
            ts[i] = td1.getFieldType(j);
            fs[i] = td1.getFieldName(j);
        }
        for (int j = 0; j < td2.numFields(); j++,i++) {
            ts[i] = td2.getFieldType(j);
            fs[i] = td2.getFieldName(j);
        }
        return new TupleDesc(ts,fs);
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here

        child1.open();
        child2.open();
        while (child1.hasNext()){
            child1Tups.add(child1.next());
        }
        while (child2.hasNext()){
            child2Tups.add(child2.next());
        }
        child1It = child1Tups.iterator();
        child2It = child2Tups.iterator();
        while (child1It.hasNext()){
            Tuple next1 = child1It.next();
            while (child2It.hasNext()){
                Tuple next2 = child2It.next();
                if(p.filter(next1,next2)){
                    Tuple t = new Tuple(getTupleDesc());
                    int i = 0;
                    Iterator<Field> fields1 = next1.fields();
                    while (fields1.hasNext()){
                        t.setField(i,fields1.next());
                        i++;
                    }

                    Iterator<Field> fields2 = next2.fields();
                    while (fields2.hasNext()){
                        t.setField(i,fields2.next());
                        i++;
                    }
                    tups.add(t);
                }
            }
            child2It = child2Tups.iterator();
        }
        it = tups.iterator();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        it = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        it = tups.iterator();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (it.hasNext()){
            return it.next();
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child1,child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child1 = children[0];
        this.child2 = children[1];
    }

}
