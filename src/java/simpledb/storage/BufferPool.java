package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.index.BTreePage;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.sql.SQLOutput;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * for LRU
     */
    class Node{
        Page page;
        Node next;
        Node pre;

        public Node(Page page) {
            this.page = page;
        }
    }

    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int pageCount;

    private int numPages;

    Page[] pages;

    ConcurrentHashMap<PageId,Node> map;

    Node fakeHead;

    Node fakeTail;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.pageCount = 0;
        this.numPages = numPages;
        pages = new Page[numPages];
        map = new ConcurrentHashMap<>();
        this.fakeHead = new Node(null);
        this.fakeTail = new Node(null);
        fakeHead.next = fakeTail;
        fakeTail.pre = fakeHead;

    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    private void deleteNode(Node node){
        if(node == null || node == fakeHead || node == fakeTail){
            return;
        }
        this.pageCount--;
        Node pre = node.pre;
        Node next = node.next;
        pre.next = next;
        next.pre = pre;
        map.remove(node.page.getId());
        for (int i = 0; i < pages.length; i++) {
            if(pages[i]!=null&&pages[i].getId().equals(node.page.getId())){
                pages[i] = null;
                return;
            }
        }
    }

    private void addToHead(Node node){
        this.pageCount++;
        Node next = fakeHead.next;
        fakeHead.next = node;
        node.next = next;
        node.pre = fakeHead;
        next.pre = node;
        map.put(node.page.getId(),node);
        for (int i = 0; i < pages.length; i++) {
            if(pages[i] == null){
                pages[i] = node.page;
                return;
            }
        }
    }
    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        if(perm.equals(Permissions.READ_ONLY)){
            while (!LockManager.getReadLock(pid,tid)){
                //阻塞
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }else{
            while (!LockManager.getWriteLock(pid,tid)){
                //阻塞
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        //先在缓冲区找,找到了先将其移动到头部，然后直接返回
        for (int i = 0; i < pages.length; i++) {
                if (pages[i] != null && pages[i].getId().equals(pid)) {
                    synchronized (pages) {
                        if (pages[i] != null && pages[i].getId().equals(pid)) {
                            Node node = map.get(pages[i].getId());
                            deleteNode(node);
                            addToHead(node);

                            return pages[i];
                        }
                    }

                }

        }

        //找不到就读取页,然后加入缓冲区头部并返回
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = file.readPage(pid);
        if (pageCount >= numPages) {
            //缓冲区已满，使用策略从缓冲区丢弃一个page
            evictPage();
        }
        addToHead(new Node(page));
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        LockManager.releaseLock(pid,tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return LockManager.holdsLock(tid,p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        LockManager.releaseAllLock(tid);

        if(commit){
            //flush dirty pages associated to the transaction to disk
            for (int i = 0; i < pages.length; i++) {
                if(pages[i]!=null && pages[i].isDirty()!=null && pages[i].isDirty().equals(tid)){
                    flushPage(i);
                }
            }
        }else{
            //revert any changes made by the transaction by restoring the page to its on-disk state.
            for (int i = 0; i < pages.length; i++) {
                if(pages[i]!=null && pages[i].isDirty()!=null && pages[i].isDirty().equals(tid)){
                    pages[i] = pages[i].getBeforeImage();
                    Node node = map.get(pages[i].getId());
                    if(node!=null){
                        node.page = pages[i].getBeforeImage();
                    }

                }
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {

        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        //下面的插入并没有写入到磁盘
        List<Page> affectedPages = file.insertTuple(tid, t);

        replacePages(tid, affectedPages);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().pid.getTableId());
        List<Page> affectedPages = file.deleteTuple(tid, t);
        replacePages(tid, affectedPages);
    }

    private void replacePages(TransactionId tid, List<Page> affectedPages) throws TransactionAbortedException, DbException, IOException {
        for (Page affectedPage : affectedPages) {
            Page page = getPage(tid, affectedPage.getId(), Permissions.READ_WRITE);
//            if(page.isDirty()!=null){
//                flushPage(page.getId());
//            }
            affectedPage.markDirty(true,tid);
            map.get(page.getId()).page = affectedPage;

            for (int i = 0; i < pages.length; i++) {
                if(pages[i]!=null&&pages[i].getId().equals(page.getId())){
                    pages[i] = affectedPage;
                }
            }
        }
    }
    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (int i = 0; i < pages.length; i++) {
            if(pages[i]!=null&&pages[i].isDirty()!=null){
                int tableId = pages[i].getId().getTableId();
                DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
                // append an update record to the log, with
                // a before-image and after-image.
                TransactionId dirtier = pages[i].isDirty();
                if (dirtier != null){
                    Database.getLogFile().logWrite(dirtier, pages[i].getBeforeImage(), pages[i]);
                    Database.getLogFile().force();
                }
                databaseFile.writePage(pages[i]);
                pages[i].markDirty(false,null);
                pages[i].setBeforeImage();
            }
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        Node node = map.get(pid);
        deleteNode(node);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        for (int i = 0; i < pages.length; i++) {
            if(pages[i]!=null&&pages[i].getId().equals(pid)){
                int tableId = pages[i].getId().getTableId();
                DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
                // append an update record to the log, with
                // a before-image and after-image.
                TransactionId dirtier = pages[i].isDirty();
                if (dirtier != null){
                    Database.getLogFile().logWrite(dirtier, pages[i].getBeforeImage(), pages[i]);
                    Database.getLogFile().force();
                }

                databaseFile.writePage(pages[i]);
                pages[i].markDirty(false,null);
                pages[i].setBeforeImage();
            }
        }
    }

    private synchronized  void flushPage(int i){

        if(pages[i]!=null){
            int tableId = pages[i].getId().getTableId();
            DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
            try {
                // append an update record to the log, with
                // a before-image and after-image.
                TransactionId dirtier = pages[i].isDirty();
                if (dirtier != null){
                    Database.getLogFile().logWrite(dirtier, pages[i].getBeforeImage(), pages[i]);
                    Database.getLogFile().force();
                }
                databaseFile.writePage(pages[i]);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            pages[i].markDirty(false,null);
            pages[i].setBeforeImage();
        }

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {

        Node cur = fakeTail.pre;
        while (cur!=fakeHead && cur.page.isDirty()!=null){
            //由于no steal  所以不能抛弃脏页
            cur = cur.pre;
        }

        if(cur == fakeHead){
            //所有的页都是脏页
            throw new DbException("All Pages are dirty in bufferPool");
        }
        Page page = cur.page;
        discardPage(page.getId());
    }

}
