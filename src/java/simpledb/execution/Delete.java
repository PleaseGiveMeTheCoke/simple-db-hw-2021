package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    TransactionId t;
    OpIterator child;
    List<Tuple> deleteTuples;
    Iterator<Tuple> it;
    BufferPool bufferPool;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.t = t;
        this.child = child;
        this.bufferPool = Database.getBufferPool();
    }

    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        int count = 0;
        while (child.hasNext()){
            Tuple next = child.next();
            try {
                bufferPool.deleteTuple(t,next);
                count++;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        deleteTuples = new ArrayList<>();
        Tuple tuple = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
        tuple.setField(0,new IntField(count));
        deleteTuples.add(tuple);
        it = deleteTuples.iterator();
        super.open();
    }

    public void close() {
        it = null;
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        it = deleteTuples.iterator();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(it == null || !it.hasNext()){
            return null;
        }
        return it.next();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }

}
