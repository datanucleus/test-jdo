package org.datanucleus.test;

import org.junit.*;
import javax.jdo.*;

import static org.junit.Assert.*;
import mydomain.model.*;
import org.datanucleus.util.NucleusLogger;

public class SimpleTest
{
    @Test
    public void testSimple()
    {
        NucleusLogger.GENERAL.info(">> test START");
        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory("MyTest");

        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();
        try
        {
            tx.begin();

            // [INSERT code here to persist object required for testing]
            tx.commit();
        }
        catch (Throwable thr)
        {
            NucleusLogger.GENERAL.error(">> Exception thrown persisting data", thr);
            fail("Failed to persist data : " + thr.getMessage());
        }
        finally 
        {
            if (tx.isActive())
            {
                tx.rollback();
            }
            pm.close();
        }

        pmf.close();
        NucleusLogger.GENERAL.info(">> test END");
    }
}
