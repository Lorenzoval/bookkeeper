package org.apache.bookkeeper.net;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class NetworkTopologyImplAddOnEmptyTest {
    private static final String VALID_NAME = "hostname";
    private static final String INVALID_NAME = "/hostname";
    private static final String VALID_LOCATION = "/location";
    private static final String INVALID_LOCATION = "location";
    private final int leaves;
    private final int innerNodes;
    private final boolean exceptionExpected;
    private final Node input;
    private NetworkTopologyImpl sut;

    public NetworkTopologyImplAddOnEmptyTest(int leaves, int innerNodes, boolean exceptionExpected, Node input) {
        this.leaves = leaves;
        this.innerNodes = innerNodes;
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

    @Before
    public void init() {
        this.sut = new NetworkTopologyImpl();
    }

    @Test
    public void testAdd() {
        boolean check;
        try {
            sut.add(input);
            if (this.exceptionExpected)
                Assert.fail("Exception not thrown");
            check = sut.getNumOfLeaves() == this.leaves;
            check &= sut.getNumOfRacks() == this.innerNodes;
            if (this.leaves > 0)
                check &= sut.contains(input);
            Assert.assertTrue(check);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("Exception thrown", this.exceptionExpected);
        }
    }

    public static class TestNode extends NodeBase {
        public void setName(String name) {
            this.name = name;
        }
    }

}
