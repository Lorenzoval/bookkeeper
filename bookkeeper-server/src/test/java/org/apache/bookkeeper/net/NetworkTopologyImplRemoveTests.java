package org.apache.bookkeeper.net;

import static org.apache.bookkeeper.net.utils.NodeUtils.*;
import static org.apache.bookkeeper.net.utils.NodeUtils.createNode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@RunWith(Enclosed.class)
public class NetworkTopologyImplRemoveTests {
    private static final String HELLO = "hello";

    @RunWith(Parameterized.class)
    public static class NetworkTopologyImplRemoveTest {
        private final int leaves;
        private final int racks;
        private final boolean exceptionExpected;
        private final Node input;
        private NetworkTopologyImpl sut;

        public NetworkTopologyImplRemoveTest(int leaves, int innerNodes, boolean exceptionExpected, Node input) {
            this.leaves = leaves;
            this.racks = innerNodes;
            this.exceptionExpected = exceptionExpected;
            this.input = input;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            return Arrays.asList(new Object[][]{
                    {3, 2, false, null},
                    {2, 2, false, createNode(VALID_NAME, VALID_LOCATION + VALID_LOCATION2)},
                    {3, 2, true, new NetworkTopologyImpl.InnerNode(VALID_LOCATION2.substring(1), VALID_LOCATION)},
                    {3, 2, false, createNode(HELLO, VALID_LOCATION)},
                    {3, 2, true, new NetworkTopologyImpl.InnerNode(HELLO, VALID_LOCATION)},
                    // Test Fails
                    // {3, 2, true, createNode(VALID_NAME, INVALID_LOCATION)},
                    {2, 1, false, createNode(VALID_NAME, VALID_LOCATION2 + VALID_LOCATION)},
                    // Test Fails
                    // {3, 2, true, createNode(VALID_LOCATION2.substring(1), VALID_LOCATION)}
            });
        }

        @Before
        public void init() {
            this.sut = new NetworkTopologyImpl();
            sut.add(createNode(VALID_NAME, VALID_LOCATION + VALID_LOCATION2));
            sut.add(createNode(VALID_NAME2, VALID_LOCATION + VALID_LOCATION2));
            sut.add(createNode(VALID_NAME, VALID_LOCATION2 + VALID_LOCATION));
        }

        @Test
        public void testRemove() {
            boolean check;
            int leavesBeforeAdd = sut.getNumOfLeaves();
            try {
                sut.remove(input);
                if (this.exceptionExpected)
                    Assert.fail("Exception not thrown");
                check = sut.getNumOfLeaves() == this.leaves;
                check &= sut.getNumOfRacks() == this.racks;
                if (this.leaves < leavesBeforeAdd)
                    check &= !sut.contains(input);
                check &= !((ReentrantReadWriteLock)sut.netlock).isWriteLocked();
                Assert.assertTrue(check);
            } catch (IllegalArgumentException e) {
                Assert.assertFalse("Lock not unlocked after exception",
                        ((ReentrantReadWriteLock) sut.netlock).isWriteLocked());
                Assert.assertTrue("Exception thrown", this.exceptionExpected);
            }
        }
    }

    @Ignore
    public static class MiscTests {
        NetworkTopologyImpl sut;

        @Before
        public void init() {
            this.sut = new NetworkTopologyImpl();
        }

        @Test
        public void removeInnerAddLeafTest() {
            sut.add(createNode(VALID_NAME, VALID_LOCATION + VALID_LOCATION2));
            sut.add(createNode(VALID_NAME2, VALID_LOCATION + VALID_LOCATION2));
            sut.add(createNode(VALID_NAME, VALID_LOCATION2 + VALID_LOCATION));
            sut.remove(createNode(VALID_LOCATION2.substring(1), VALID_LOCATION));
            try {
                sut.add(createNode(VALID_NAME, VALID_LOCATION + VALID_LOCATION2));
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail("Can't add node");
            }
        }
    }

}
