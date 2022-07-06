package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator extends AbstractDbFileIterator{
    int curPageNum;
    Iterator<Tuple> it;
    HeapPage curPage;
    HeapFile file;
    TransactionId tid;
    int pageNum;
    BufferPool bufferPool;
    public HeapFileIterator(HeapFile file, TransactionId tid) {
        this.file = file;
        this.tid = tid;
        this.pageNum = file.numPages();
        this.bufferPool = Database.getBufferPool();
        this.curPageNum = 0;
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        while (it != null && !it.hasNext()) {
            if(curPageNum == pageNum-1){
                it = null;
            }else{
                curPageNum+=1;
                curPage = (HeapPage)bufferPool.getPage(tid, new HeapPageId(file.getId(), curPageNum), Permissions.READ_ONLY);
                it = curPage.iterator();
            }
        }
        if(it == null){
            return null;
        }
        return it.next();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        curPage = (HeapPage)bufferPool.getPage(tid, new HeapPageId(file.getId(), 0), Permissions.READ_ONLY);
        it = curPage.iterator();
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
        curPage = null;
        curPageNum = 0;
    }

}
