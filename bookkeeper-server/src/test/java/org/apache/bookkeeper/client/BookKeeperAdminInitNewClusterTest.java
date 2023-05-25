package org.apache.bookkeeper.client;

import org.junit.Assert;
import org.junit.Test;

public class BookKeeperAdminInitNewClusterTest {

    @Test
    public void testInitNewCluster() {
        try {
            BookKeeperAdmin.initNewCluster(null);
            Assert.fail("Exception not thrown");
        } catch (Exception ignored) {

        }
    }

}
