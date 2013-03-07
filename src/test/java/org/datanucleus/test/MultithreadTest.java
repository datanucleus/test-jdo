package org.datanucleus.test;

import org.junit.*;
import javax.jdo.*;

import static org.junit.Assert.*;
import mydomain.model.*;
import org.datanucleus.util.NucleusLogger;

public class MultithreadTest
{
    @Test
    public void testMulti()
    {
        NucleusLogger.GENERAL.info(">> test START");
        final PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory("MyTest");

        try
        {
            // Persist some data
            NucleusLogger.GENERAL.debug(">> Persisting data");
            PersistenceManager pm = pmf.getPersistenceManager();
            Transaction tx = pm.currentTransaction();
            try
            {
                tx.begin();

                // [Add persistence of sample data for the test]

                tx.commit();
            }
            catch (Throwable thr)
            {
                NucleusLogger.GENERAL.error("Exception persisting objects", thr);
                fail("Exception persisting data : " + thr.getMessage());
            }
            finally
            {
                if (tx.isActive())
                {
                    tx.rollback();
                }
                pm.close();
            }
            NucleusLogger.GENERAL.debug(">> Persisted data");

            // Create the Threads
            int THREAD_SIZE = 500;
            final String[] threadErrors = new String[THREAD_SIZE];
            Thread[] threads = new Thread[THREAD_SIZE];
            for (int i = 0; i < THREAD_SIZE; i++)
            {
                final int threadNo = i;
                threads[i] = new Thread(new Runnable()
                {
                    public void run()
                    {
                        String errorMsg = performTest(pmf);
                        threadErrors[threadNo] = errorMsg;
                    }
                });
            }

            // Run the threads
            NucleusLogger.GENERAL.debug(">> Starting threads");
            for (int i = 0; i < THREAD_SIZE; i++)
            {
                threads[i].start();
            }
            for (int i = 0; i < THREAD_SIZE; i++)
            {
                try
                {
                    threads[i].join();
                }
                catch (InterruptedException e)
                {
                    fail(e.getMessage());
                }
            }
            NucleusLogger.GENERAL.debug(">> Completed threads");

            // Process any errors from Threads and fail the test if any failed
            for (String error : threadErrors)
            {
                if (error != null)
                {
                    fail(error);
                }
            }
        }
        finally
        {
            // [Clean up data]
        }

        pmf.close();
        NucleusLogger.GENERAL.info(">> test END");
    }

    /**
     * Method to perform the test for a Thread.
     * @param pmf The PersistenceManagerFactory
     * @return A string which is null if the PM operations are successful
     */
    protected String performTest(PersistenceManagerFactory pmf)
    {
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();
        try
        {
            tx.begin();

            // [Add persistence code to perform what is needed by this PM]

            tx.commit();
        }
        catch (Throwable thr)
        {
            NucleusLogger.GENERAL.error("Exception performing test", thr);
            return "Exception performing test : " + thr.getMessage();
        }
        finally
        {
            if (tx.isActive())
            {
                tx.rollback();
            }
            pm.close();
        }
        return null;
    }
}
