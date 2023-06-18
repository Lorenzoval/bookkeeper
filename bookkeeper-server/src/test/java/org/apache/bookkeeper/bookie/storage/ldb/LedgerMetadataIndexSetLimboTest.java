package org.apache.bookkeeper.bookie.storage.ldb;

import com.google.protobuf.ByteString;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;

@RunWith(Parameterized.class)
public class LedgerMetadataIndexSetLimboTest {

    private final boolean expectedValue;
    private final long ledgerId;
    private final boolean concurrentDelete;
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    KeyValueStorage.CloseableIterator<Map.Entry<byte[], byte[]>> closeableIterator;
    @Mock
    KeyValueStorage keyValueStorage;
    @Mock
    KeyValueStorageFactory keyValueStorageFactory;
    private LedgerMetadataIndex sut;
    private Map<byte[], byte[]> map;
    private Iterator<Map.Entry<byte[], byte[]>> iterator;

    public LedgerMetadataIndexSetLimboTest(boolean expectedValue, Class<Throwable> exceptionClass, long ledgerId,
                                           boolean concurrentDelete) {
        this.expectedValue = expectedValue;
        if (exceptionClass != null) {
            this.expectedException.expect(exceptionClass);
        }
        this.ledgerId = ledgerId;
        this.concurrentDelete = concurrentDelete;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                {false, IOException.class, 0, false},
                {true, null, 1, false},
                {false, null, 2, false},
                {true, null, 1, true}
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

        LedgerData notLimbo = LedgerData.newBuilder().setExists(true).setFenced(false)
                .setMasterKey(ByteString.copyFrom(new byte[0])).setLimbo(false).build();
        ByteBuffer bufferNotLimbo = ByteBuffer.allocate(Long.BYTES);
        bufferNotLimbo.putLong(1);
        map.put(bufferNotLimbo.array(), notLimbo.toByteArray());
        LedgerData limbo = LedgerData.newBuilder(notLimbo).setLimbo(true).build();
        ByteBuffer bufferLimbo = ByteBuffer.allocate(Long.BYTES);
        bufferLimbo.putLong(2);
        map.put(bufferLimbo.array(), limbo.toByteArray());

        this.sut = new LedgerMetadataIndex(new ServerConfiguration(), this.keyValueStorageFactory, "path",
                new NullStatsLogger());

        if (concurrentDelete) {
            this.sut = Mockito.spy(this.sut);
            Mockito.when(this.sut.get(this.ledgerId)).then(invocation -> {
                LedgerData ledgerData = (DbLedgerStorageDataFormats.LedgerData) invocation.callRealMethod();
                this.sut.delete(this.ledgerId);
                return ledgerData;
            });
        }
    }

    @Test
    public void testSetLimbo() throws IOException {
        boolean result = sut.setLimbo(ledgerId);
        Assert.assertEquals(result, expectedValue);
    }

}
