package org.apache.bookkeeper.bookie.storage.ldb;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
    private static final String MASTER_KEY = "masterKey";

    private final boolean emptyDB;
    private final long ledgerId;
    private final Class<Throwable> exceptionClass;
    private final Class<Throwable> exceptionClass2;
    private final ChangeType changeType;
    private final LedgerData expected1;
    private final LedgerData expected2;
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();
    @Mock
    KeyValueStorage.CloseableIterator<Map.Entry<byte[], byte[]>> closeableIterator;
    @Mock
    KeyValueStorage keyValueStorage;
    @Mock
    KeyValueStorageFactory keyValueStorageFactory;
    private LedgerMetadataIndex sut;
    private Map<byte[], byte[]> map;
    private Iterator<Map.Entry<byte[], byte[]>> iterator;

    public LedgerMetadataIndexTest(Class<Throwable> exceptionClass, boolean emptyDB, long ledgerId,
                                   Class<Throwable> exceptionClass2, ChangeType changeType) {
        this.exceptionClass = exceptionClass;
        this.emptyDB = emptyDB;
        this.ledgerId = ledgerId;
        this.exceptionClass2 = exceptionClass2;
        this.changeType = changeType;
        this.expected1 = LedgerData.newBuilder().setExists(true).setFenced(false)
                .setMasterKey(ByteString.copyFrom(new byte[0])).build();
        this.expected2 = expectedOutputFromChangeType(changeType);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                {IOException.class, true, 1, null, ChangeType.NULL},
                {IOException.class, false, -1, null, ChangeType.NULL},
                {IOException.class, false, 0, null, ChangeType.NULL},
                {null, false, 1, null, ChangeType.SET},
                {null, false, 1, null, ChangeType.SET_FENCED},
                {null, false, 1, null, ChangeType.SET_MASTER_KEY},
                {null, false, 1, null, ChangeType.SET_EXPLICIT_LAC},
                {null, false, 1, null, ChangeType.SET_LIMBO},
                {null, false, 1, IOException.class, ChangeType.DELETE},
        });
    }

    private LedgerData expectedOutputFromChangeType(ChangeType changeType) {
        ByteBuffer lacToAdd = ByteBuffer.allocate(Long.BYTES);
        lacToAdd.putLong(123);
        switch (changeType) {
            case SET:
                return LedgerData.newBuilder().setExists(true).setFenced(true)
                        .setMasterKey(ByteString.copyFrom(MASTER_KEY.getBytes()))
                        .setExplicitLac(ByteString.copyFrom(lacToAdd)).setLimbo(true).build();
            case SET_FENCED:
                return LedgerData.newBuilder().setExists(true).setFenced(true)
                        .setMasterKey(ByteString.copyFrom(new byte[0])).build();
            case SET_MASTER_KEY:
                return LedgerData.newBuilder().setExists(true).setFenced(false)
                        .setMasterKey(ByteString.copyFrom(MASTER_KEY.getBytes())).build();
            case SET_EXPLICIT_LAC:
                return LedgerData.newBuilder().setExists(true).setFenced(false)
                        .setMasterKey(ByteString.copyFrom(new byte[0]))
                        .setExplicitLac(ByteString.copyFrom(lacToAdd)).build();
            case SET_LIMBO:
                return LedgerData.newBuilder().setExists(true).setFenced(false)
                        .setMasterKey(ByteString.copyFrom(new byte[0])).setLimbo(true).build();
            case DELETE:
            case NULL:
            default:
                return null;
        }
    }

    private void executeChangeType() throws IOException {
        ByteBuffer lacToAdd;
        ByteBuf byteBuf;
        switch (changeType) {
            case SET:
                lacToAdd = ByteBuffer.allocate(Long.BYTES);
                lacToAdd.putLong(123);
                LedgerData ledgerData = LedgerData.newBuilder().setExists(true).setFenced(true)
                        .setMasterKey(ByteString.copyFrom(MASTER_KEY.getBytes()))
                        .setExplicitLac(ByteString.copyFrom(lacToAdd))
                        .setLimbo(true).build();
                sut.set(1, ledgerData);
                break;
            case SET_FENCED:
                sut.setFenced(1);
                break;
            case SET_MASTER_KEY:
                sut.setMasterKey(1, MASTER_KEY.getBytes());
                break;
            case SET_EXPLICIT_LAC:
                lacToAdd = ByteBuffer.allocate(Long.BYTES);
                lacToAdd.putLong(123);
                byteBuf = Unpooled.wrappedBuffer(lacToAdd);
                sut.setExplicitLac(1, byteBuf);
                break;
            case SET_LIMBO:
                sut.setLimbo(1);
                break;
            case DELETE:
                sut.delete(1);
                break;
            case NULL:
            default:
                break;
        }
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
            map.put(buffer.array(), expected1.toByteArray());
        }

        this.sut = new LedgerMetadataIndex(new ServerConfiguration(), this.keyValueStorageFactory, "path",
                new NullStatsLogger());
    }

    @Test
    public void testLedgerMetadataIndex() throws IOException {
        try {
            LedgerData result = sut.get(this.ledgerId);
            if (this.exceptionClass != null)
                Assert.fail("Exception not thrown");
            Assert.assertEquals(result, this.expected1);
        } catch (Exception e) {
            if (this.exceptionClass == null) {
                Assert.fail("Exception thrown");
                e.printStackTrace();
            }
            Assert.assertTrue(this.exceptionClass.isInstance(e));
        }
        if (changeType == null || changeType == ChangeType.NULL)
            return;
        executeChangeType();
        try {
            LedgerData result = sut.get(1);
            if (this.exceptionClass2 != null)
                Assert.fail("Exception not thrown");
            Assert.assertEquals(result, this.expected2);
        } catch (Exception e) {
            if (this.exceptionClass2 == null) {
                Assert.fail("Exception thrown");
                e.printStackTrace();
            }
            Assert.assertTrue(this.exceptionClass2.isInstance(e));
        }
    }

    private enum ChangeType {
        NULL,
        SET,
        SET_FENCED,
        SET_MASTER_KEY,
        SET_EXPLICIT_LAC,
        SET_LIMBO,
        DELETE
    }

}
