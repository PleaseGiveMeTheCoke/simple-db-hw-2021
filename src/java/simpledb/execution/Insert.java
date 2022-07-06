package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    List<Tuple> tuples;

    Iterator<Tuple> it;

    TransactionId t;

    OpIterator child;

    int tableId;

    DbFile file;

    BufferPool  bufferPool;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        this.file = Database.getCatalog().getDatabaseFile(tableId);
        this.bufferPool = Database.getBufferPool();
        this.tuples = new ArrayList<>();
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return file.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        int count = 0;
        while (child.hasNext()){
            try {
                bufferPool.insertTuple(t,tableId,child.next());
                count++;
            }catch (IOException e){
                System.out.println("tableId = "+tableId+" file can't access");
            }
        }
        Tuple t = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
        t.setField(0,new IntField(count));
        tuples.add(t);
        it = tuples.iterator();
        super.open();
    }

    public void close() {
        tuples = new ArrayList<>();
        it = null;
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        it = tuples.iterator();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
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
