package org.apache.bookkeeper.net.utils;

import org.apache.bookkeeper.net.Node;
import org.apache.bookkeeper.net.NodeBase;

import java.util.ArrayList;
import java.util.List;

public class NodeUtils {
    public static final String VALID_NAME = "hostname";
    public static final String VALID_NAME2 = "hostname2";
    public static final String INVALID_NAME = "/hostname";
    public static final String VALID_LOCATION = "/location";
    public static final String VALID_LOCATION2 = "/location2";
    public static final String INVALID_LOCATION = "location";

    private NodeUtils() {
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

    public static class TestNode extends NodeBase {
        public void setName(String name) {
            this.name = name;
        }
    }

}
