package simpledb.storage;

import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.Iterator;

public class HeapFileIterator extends AbstractDbFileIterator{

    ArrayList<HeapPage> pages;
    Iterator<Tuple> it;
    public HeapFileIterator(ArrayList<HeapPage> pages) {
        this.pages = pages;
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (it != null && !it.hasNext())
            it = null;
        if(it == null){
            return null;
        }
        return it.next();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        ArrayList<Tuple> ar = new ArrayList<>();
        for (HeapPage page : pages) {
            Iterator<Tuple> iterator = page.iterator();
            while (iterator.hasNext()){
                ar.add(iterator.next());
            }
        }
        it = ar.iterator();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public void close() {
        super.close();
        it = null;
    }
}
