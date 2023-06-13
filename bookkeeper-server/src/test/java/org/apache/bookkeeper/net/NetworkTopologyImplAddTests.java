package org.apache.bookkeeper.net;

import static org.apache.bookkeeper.net.utils.NodeUtils.*;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@RunWith(Enclosed.class)
public class NetworkTopologyImplAddTests {
    private static final String ON_LEAF = VALID_LOCATION + "/" + VALID_NAME;
    private static final String ON_EXISTING_LOCATION = VALID_LOCATION;
    private static final String ON_NEW_LOCATION = VALID_LOCATION2;

    @RunWith(Parameterized.class)
    public static class NetworkTopologyImplAddOnEmptyTest {
        private final int leaves;
        private final int racks;
        private final boolean exceptionExpected;
        private final Node input;
        private NetworkTopologyImpl sut;

        public NetworkTopologyImplAddOnEmptyTest(int leaves, int innerNodes, boolean exceptionExpected, Node input) {
            this.leaves = leaves;
            this.racks = innerNodes;
            this.exceptionExpected = exceptionExpected;
            this.input = input;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            return Arrays.asList(new Object[][]{
                    {0, 0, false, null},
                    {1, 1, false, createNode(VALID_NAME, VALID_LOCATION)},
                    // Test Fails
                    // {0, 0, true, createNode(INVALID_NAME, VALID_LOCATION)},
                    // Test Fails
                    // {0, 0, true, createNode("", VALID_LOCATION)},
                    // Test Fails
                    // {0, 0, true, createNode(null, VALID_LOCATION)},
                    {0, 0, true, createNode(VALID_NAME, INVALID_LOCATION)},
                    {0, 0, true, createNode(INVALID_NAME, INVALID_LOCATION)},
                    {0, 0, true, createNode("", INVALID_LOCATION)},
                    {0, 0, true, createNode(null, INVALID_LOCATION)},
                    {0, 0, true, createNode(VALID_NAME, "")},
                    {0, 0, true, createNode(INVALID_NAME, "")},
                    {0, 0, true, createNode("", "")},
                    {0, 0, true, createNode(null, "")},
                    {0, 0, true, createNode(VALID_NAME, null)},
                    {0, 0, true, createNode(INVALID_NAME, null)},
                    {0, 0, true, createNode("", null)},
                    {0, 0, true, createNode(null, null)},
                    {0, 0, true, new NetworkTopologyImpl.InnerNode(VALID_NAME, VALID_LOCATION)}
            });
        }

        @Before
        public void init() {
            this.sut = new NetworkTopologyImpl();
        }

        @Test
        public void testAddOnEmpty() {
            boolean check;
            int leavesBeforeAdd = sut.getNumOfLeaves();
            try {
                sut.add(input);
                if (this.exceptionExpected)
                    Assert.fail("Exception not thrown");
                check = sut.getNumOfLeaves() == this.leaves;
                check &= sut.getNumOfRacks() == this.racks;
                if (this.leaves > leavesBeforeAdd)
                    check &= sut.contains(input);
                check &= !((ReentrantReadWriteLock) sut.netlock).isWriteLocked();
                Assert.assertTrue(check);
            } catch (IllegalArgumentException e) {
                Assert.assertFalse("Lock not unlocked after exception",
                        ((ReentrantReadWriteLock) sut.netlock).isWriteLocked());
                Assert.assertTrue("Exception thrown", this.exceptionExpected);
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class NetworkTopologyImplAddOnNonEmptyTest {
        private final int leaves;
        private final int racks;
        private final Class<Throwable> exceptionClass;
        private final Node input;
        private final List<Node> alreadyAddedNodes;
        @Rule
        public ExpectedException expected = ExpectedException.none();
        private NetworkTopologyImpl sut;

        public NetworkTopologyImplAddOnNonEmptyTest(int leaves, int innerNodes, Class<Throwable> exceptionClass, Node input,
                                                    List<Node> alreadyAddedNodes) {
            this.leaves = leaves;
            this.racks = innerNodes;
            this.exceptionClass = exceptionClass;
            this.input = input;
            this.alreadyAddedNodes = alreadyAddedNodes;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            return Arrays.asList(new Object[][]{
                    // Test Fails
                    // {1, 1, IllegalArgumentException.class, createNode(VALID_NAME2, ON_LEAF), createList(0)},
                    {1, 1, null, createNode(VALID_NAME, ON_EXISTING_LOCATION), createList(0)},
                    {2, 2, null, createNode(VALID_NAME, ON_NEW_LOCATION), createList(0)},
                    {2, 1, null, createNode(VALID_NAME2, ON_EXISTING_LOCATION), createList(0)},
                    {2, 1, null, createNode("", ON_EXISTING_LOCATION), createList(0)},
                    {2, 1, null, createNode(null, ON_EXISTING_LOCATION), createList(0)},
                    {1, 1, null, null, createList(0)},
                    {1, 1, IllegalArgumentException.class, new NetworkTopologyImpl.InnerNode(VALID_NAME2, ON_EXISTING_LOCATION), createList(0)},
                    {2, 1, null, createNode(VALID_NAME, ON_EXISTING_LOCATION), createList(1)},
                    // Test Fails
                    // {2, 1, null, createNode(VALID_NAME, ON_EXISTING_LOCATION), createList(2)},
                    {3, 1, null, createNode(VALID_NAME2, ON_EXISTING_LOCATION), createList(3)},
                    // Test Fails
                    // {3, 1, null, createNode(VALID_NAME2, ON_EXISTING_LOCATION), createList(4)},
                    // Test Fails
                    // {3, 1, null, createNode(VALID_NAME2, ON_EXISTING_LOCATION), createList(5)},
                    // Test Fails
                    // {4, 1, null, createNode(VALID_NAME2, ON_EXISTING_LOCATION), createList(6)},
                    // White Box starts here
                    {2, 2, NetworkTopologyImpl.InvalidTopologyException.class, createNode(VALID_NAME2, VALID_LOCATION + VALID_LOCATION2), createList(0)}
            });
        }

        @Before
        public void init() {
            this.sut = new NetworkTopologyImpl();
            for (Node node : this.alreadyAddedNodes)
                sut.add(node);
        }

        @Test
        public void testAddOnNonEmpty() {
            boolean check;
            int leavesBeforeAdd = sut.getNumOfLeaves();
            if (exceptionClass != null) {
                expected.expect(exceptionClass);
            }
            try {
                sut.add(input);
                check = sut.getNumOfLeaves() == this.leaves;
                check &= sut.getNumOfRacks() == this.racks;
                if (this.leaves > leavesBeforeAdd)
                    check &= sut.contains(input);
                check &= !((ReentrantReadWriteLock) sut.netlock).isWriteLocked();
                Assert.assertTrue(check);
            } catch (Exception e) {
                Assert.assertFalse(((ReentrantReadWriteLock) sut.netlock).isWriteLocked());
                throw e;
            }
        }
    }

}
