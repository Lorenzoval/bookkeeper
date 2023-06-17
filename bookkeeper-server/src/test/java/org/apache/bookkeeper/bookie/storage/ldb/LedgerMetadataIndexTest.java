package org.apache.bookkeeper.bookie.storage.ldb;

import com.google.protobuf.ByteString;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(Parameterized.class)
public class LedgerMetadataIndexTest {

    private final boolean emptyDB;
    private final long ledgerId;
    private final Class<Throwable> exceptionClass;
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();
    @Mock
    KeyValueStorage.CloseableIterator<Map.Entry<byte[], byte[]>> closeableIterator;
    @Mock
    KeyValueStorage keyValueStorage;
    @Mock
    KeyValueStorageFactory keyValueStorageFactory;
    private LedgerData ledgerData;
    private LedgerMetadataIndex sut;
    private Map<byte[], byte[]> map;
    private Iterator<Map.Entry<byte[], byte[]>> iterator;

    public LedgerMetadataIndexTest(Class<Throwable> exceptionClass, boolean emptyDB, long ledgerId) {
        this.exceptionClass = exceptionClass;
        this.emptyDB = emptyDB;
        this.ledgerId = ledgerId;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                {IOException.class, true, 1},
                {IOException.class, false, -1},
                {IOException.class, false, 0},
                {null, false, 1}
        });
    }

    @Before
    public void init() throws IOException {
        map = new HashMap<>();

        Mockito.when(keyValueStorageFactory.newKeyValueStorage(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any())).thenReturn(keyValueStorage);

        Mockito.when(keyValueStorage.iterator()).then(invocationOnMock -> {
            iterator = map.entrySet().iterator();
            return closeableIterator;
        });

        Mockito.when(closeableIterator.hasNext()).then(invocationOnMock -> iterator.hasNext());
        Mockito.when(closeableIterator.next()).then(invocationOnMock -> iterator.next());

        if (!emptyDB) {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(1);
            ledgerData = LedgerData.newBuilder().setExists(true).setFenced(false)
                    .setMasterKey(ByteString.copyFrom(new byte[0])).build();
            map.put(buffer.array(), ledgerData.toByteArray());
        }

        this.sut = new LedgerMetadataIndex(new ServerConfiguration(), this.keyValueStorageFactory, "path",
                new NullStatsLogger());
    }

    @Test
    public void testLedgerMetadataIndex() {
        try {
            LedgerData result = sut.get(this.ledgerId);
            if (this.exceptionClass != null)
                Assert.fail("Exception not thrown");
            Assert.assertEquals(result, this.ledgerData);
        } catch (Exception e) {
            if (this.exceptionClass == null) {
                Assert.fail("Exception thrown");
                e.printStackTrace();
            }
            Assert.assertTrue(this.exceptionClass.isInstance(e));
        }
    }

}
