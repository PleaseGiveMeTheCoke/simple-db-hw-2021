package simpledb.storage;

import jdk.nashorn.internal.ir.CallNode;
import simpledb.common.*;
import simpledb.index.BTreePageId;
import simpledb.index.BTreeRootPtrPage;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    File f;

    TupleDesc td;

    ArrayList<HeapPage> pages;

    DbFileIterator it;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;

        Database.getCatalog().addTable(this);
        loadPageFromFile(f);

       // this.it = new HeapFileIterator(pages);
    }
    public int getPageNum(){
        return pages.size();
    }
    private void loadPageFromFile(File f) {
        int pageSize = BufferPool.getPageSize();
        pages = new ArrayList<>();

        try {
            InputStream inputStream = new FileInputStream(f);
            while (true){
                byte[] bytes = new byte[pageSize];
                int ret = inputStream.read(bytes);
                if(ret == -1){
                    break;
                }

                HeapPage page = new HeapPage(new HeapPageId(getId(),pages.size()),bytes);
                pages.add(page);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            int num = pid.getPageNumber();
            byte[] data = new byte[BufferPool.getPageSize()];
            RandomAccessFile rf = new RandomAccessFile(f, "rw");
            rf.seek(BufferPool.getPageSize() * num);
            rf.read(data);
            rf.close();
            return new HeapPage((HeapPageId) pid, data);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
//        HeapPageId HPID = (HeapPageId) pid;
//        return pages.get(HPID.getPageNumber());

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int num = page.getId().getPageNumber();
        byte[] data = page.getPageData();
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        rf.seek(BufferPool.getPageSize()*num);
        rf.write(data);
        rf.close();
        pages.add((HeapPage) page);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return pages.size();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException,TransactionAbortedException {
        List<Page> res = new ArrayList<>();
        for (HeapPage page : pages) {
            //通过buffer pool来访问该页
            HeapPage hp = (HeapPage)Database.getBufferPool().getPage(tid, page.pid, Permissions.READ_ONLY);
            //该页有空位
            if(hp.getNumEmptySlots()!=0){
                //升级为写锁
                HeapPage hp_w = (HeapPage)Database.getBufferPool().getPage(tid, page.pid, Permissions.READ_WRITE);
                //标记脏页
                hp_w.markDirty(true,tid);
                hp_w.insertTuple(t);
                res.add(hp);
                return res;
            }else{
                //释放锁
                Database.getBufferPool().unsafeReleasePage(tid,page.pid);
            }
        }
        //所有页都满了,创建新页
        HeapPage page = new HeapPage(new HeapPageId(getId(),pages.size()));
        page.insertTuple(t);
        //将新页写入磁盘
        writePage(page);

        res.add(page);
        return res;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> res = new ArrayList<>();
        for (HeapPage page : pages) {
            HeapPage hp = (HeapPage)Database.getBufferPool().getPage(tid, page.pid, Permissions.READ_ONLY);
            if(hp.getTupleSlotNum(t.getRecordId())!=-1){
                //升级为写锁
                HeapPage hp_w = (HeapPage)Database.getBufferPool().getPage(tid, page.pid, Permissions.READ_WRITE);
                //标记脏页
                hp_w.markDirty(true,tid);
                hp.deleteTuple(t);
                res.add(hp);
                break;
            }else{
                //释放锁
                Database.getBufferPool().unsafeReleasePage(tid,page.pid);
            }
        }
        return res;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

        return new HeapFileIterator(this,tid);
    }

}

