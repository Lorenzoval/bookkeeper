package org.apache.bookkeeper.net;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Enclosed.class)
public class NetworkTopologyImplAddTests {
    private static final String VALID_NAME = "hostname";
    private static final String VALID_NAME2 = "hostname2";
    private static final String INVALID_NAME = "/hostname";
    private static final String VALID_LOCATION = "/location";
    private static final String VALID_LOCATION2 = "/location2";
    private static final String INVALID_LOCATION = "location";
    private static final String ON_LEAF = VALID_LOCATION + "/" + VALID_NAME;
    private static final String ON_EXISTING_LOCATION = VALID_LOCATION;
    private static final String ON_NEW_LOCATION = VALID_LOCATION2;

    public static Node createNode(String name, String location) {
        Node node;
        if (null == name) {
            node = new NodeBase();
            node.setNetworkLocation(location);
        } else if (name.equals(INVALID_NAME)) {
            node = new TestNode();
            ((TestNode) node).setName(name);
            node.setNetworkLocation(location);
        } else if (null == location) {
            node = new TestNode();
            ((TestNode) node).setName(name);
        } else if (location.equals(INVALID_LOCATION)) {
            node = new TestNode();
            ((TestNode) node).setName(name);
            node.setNetworkLocation(location);
        } else {
            node = new NodeBase(name, location);
        }
        return node;
    }

    public static List<Node> createList(int configuration) {
        List<Node> list = new ArrayList<>();
        switch (configuration) {
            case 0:
                list.add(createNode(VALID_NAME, VALID_LOCATION));
                break;
            case 1:
                list.add(createNode("", VALID_LOCATION));
                break;
            case 2:
                list.add(createNode(null, VALID_LOCATION));
                break;
            case 3:
                list.add(createNode(VALID_NAME, VALID_LOCATION));
                list.add(createNode("", VALID_LOCATION));
                break;
            case 4:
                list.add(createNode(VALID_NAME, VALID_LOCATION));
                list.add(createNode(null, VALID_LOCATION));
                break;
            case 5:
                list.add(createNode("", VALID_LOCATION));
                list.add(createNode(null, VALID_LOCATION));
                break;
            case 6:
                list.add(createNode(VALID_NAME, VALID_LOCATION));
                list.add(createNode("", VALID_LOCATION));
                list.add(createNode(null, VALID_LOCATION));
                break;
        }
        return list;
    }

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
                    {0, 0, true, createNode(INVALID_NAME, VALID_LOCATION)},
                    // Test Fails
                    {0, 0, true, createNode("", VALID_LOCATION)},
                    // Test Fails
                    {0, 0, true, createNode(null, VALID_LOCATION)},
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
                Assert.assertTrue(check);
            } catch (IllegalArgumentException e) {
                Assert.assertTrue("Exception thrown", this.exceptionExpected);
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class NetworkTopologyImplAddOnNonEmptyTest {
        private final int leaves;
        private final int racks;
        private final boolean exceptionExpected;
        private final Node input;
        private final List<Node> alreadyAddedNodes;
        private NetworkTopologyImpl sut;

        public NetworkTopologyImplAddOnNonEmptyTest(int leaves, int innerNodes, boolean exceptionExpected, Node input,
                                                    List<Node> alreadyAddedNodes) {
            this.leaves = leaves;
            this.racks = innerNodes;
            this.exceptionExpected = exceptionExpected;
            this.input = input;
            this.alreadyAddedNodes = alreadyAddedNodes;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            return Arrays.asList(new Object[][]{
                    // Test Fails
                    {1, 1, true, createNode(VALID_NAME2, ON_LEAF), createList(0)},
                    {1, 1, false, createNode(VALID_NAME, ON_EXISTING_LOCATION), createList(0)},
                    {2, 2, false, createNode(VALID_NAME, ON_NEW_LOCATION), createList(0)},
                    {2, 1, false, createNode(VALID_NAME2, ON_EXISTING_LOCATION), createList(0)},
                    {2, 1, false, createNode("", ON_EXISTING_LOCATION), createList(0)},
                    {2, 1, false, createNode(null, ON_EXISTING_LOCATION), createList(0)},
                    {1, 1, false, null, createList(0)},
                    {1, 1, true, new NetworkTopologyImpl.InnerNode(VALID_NAME2, ON_EXISTING_LOCATION), createList(0)},
                    {2, 1, false, createNode(VALID_NAME, ON_EXISTING_LOCATION), createList(1)},
                    // Test Fails
                    {2, 1, false, createNode(VALID_NAME, ON_EXISTING_LOCATION), createList(2)},
                    {3, 1, false, createNode(VALID_NAME2, ON_EXISTING_LOCATION), createList(3)},
                    // Test Fails
                    {3, 1, false, createNode(VALID_NAME2, ON_EXISTING_LOCATION), createList(4)},
                    // Test Fails
                    {3, 1, false, createNode(VALID_NAME2, ON_EXISTING_LOCATION), createList(5)},
                    // Test Fails
                    {4, 1, false, createNode(VALID_NAME2, ON_EXISTING_LOCATION), createList(6)}
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
            try {
                sut.add(input);
                if (this.exceptionExpected)
                    Assert.fail("Exception not thrown");
                check = sut.getNumOfLeaves() == this.leaves;
                check &= sut.getNumOfRacks() == this.racks;
                if (this.leaves > leavesBeforeAdd)
                    check &= sut.contains(input);
                Assert.assertTrue(check);
            } catch (IllegalArgumentException e) {
                Assert.assertTrue("Exception thrown", this.exceptionExpected);
            }
        }
    }

    @Ignore
    public static class TestNode extends NodeBase {
        public void setName(String name) {
            this.name = name;
        }
    }

}
