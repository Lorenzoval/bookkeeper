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

    private final DBState dbState;
    private final long ledgerIdFirstGet;
    private final Class<Throwable> exceptionClass;
    private final Class<Throwable> exceptionClassOnExecute;
    private final Class<Throwable> exceptionClass2;
    private final ChangeType changeType;
    private final long ledgerIdSecondGet;
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

    public LedgerMetadataIndexTest(Class<Throwable> exceptionClass, DBState dbState, long ledgerIdFirstGet,
                                   Class<Throwable> exceptionClassOnExecute, Class<Throwable> exceptionClass2,
                                   ChangeType changeType, long ledgerIdSecondGet) {
        this.exceptionClass = exceptionClass;
        this.dbState = dbState;
        this.ledgerIdFirstGet = ledgerIdFirstGet;
        this.exceptionClassOnExecute = exceptionClassOnExecute;
        this.exceptionClass2 = exceptionClass2;
        this.changeType = changeType;
        this.ledgerIdSecondGet = ledgerIdSecondGet;
        if (dbState == DBState.MASTER_KEY)
            this.expected1 = LedgerData.newBuilder().setExists(true).setFenced(false)
                    .setMasterKey(ByteString.copyFrom("a".getBytes())).build();
        else
            this.expected1 = LedgerData.newBuilder().setExists(true).setFenced(false)
                    .setMasterKey(ByteString.copyFrom(new byte[0])).build();
        this.expected2 = expectedOutputFromChangeType(changeType);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                {IOException.class, DBState.EMPTY, 1, null, null, ChangeType.NULL, 1},
                {IOException.class, DBState.INVALID, -1, null, null, ChangeType.NULL, 1},
                {IOException.class, DBState.VALID, 0, null, null, ChangeType.NULL, 1},
                {null, DBState.VALID, 1, null, null, ChangeType.SET, 1},
                {null, DBState.VALID, 1, null, null, ChangeType.SET_FENCED, 1},
                {null, DBState.VALID, 1, null, null, ChangeType.SET_MASTER_KEY, 1},
                {null, DBState.VALID, 1, null, null, ChangeType.SET_EXPLICIT_LAC, 1},
                {null, DBState.VALID, 1, null, null, ChangeType.SET_LIMBO, 1},
                {null, DBState.VALID, 1, null, IOException.class, ChangeType.DELETE, 1},
                {null, DBState.VALID, 1, null, null, ChangeType.SET, 2},
                {null, DBState.VALID, 1, IOException.class, null, ChangeType.SET_FENCED, 2},
                {null, DBState.VALID, 1, null, null, ChangeType.SET_MASTER_KEY, 2},
                {null, DBState.VALID, 1, null, IOException.class, ChangeType.SET_EXPLICIT_LAC, 2},
                {null, DBState.VALID, 1, IOException.class, null, ChangeType.SET_LIMBO, 2},
                {null, DBState.VALID, 1, null, IOException.class, ChangeType.DELETE, 2},
                {null, DBState.MASTER_KEY, 1, IOException.class, null, ChangeType.SET_MASTER_KEY, 1},
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
                sut.set(ledgerIdSecondGet, ledgerData);
                break;
            case SET_FENCED:
                sut.setFenced(ledgerIdSecondGet);
                break;
            case SET_MASTER_KEY:
                sut.setMasterKey(ledgerIdSecondGet, MASTER_KEY.getBytes());
                break;
            case SET_EXPLICIT_LAC:
                lacToAdd = ByteBuffer.allocate(Long.BYTES);
                lacToAdd.putLong(123);
                byteBuf = Unpooled.wrappedBuffer(lacToAdd);
                sut.setExplicitLac(ledgerIdSecondGet, byteBuf);
                break;
            case SET_LIMBO:
                sut.setLimbo(ledgerIdSecondGet);
                break;
            case DELETE:
                sut.delete(ledgerIdSecondGet);
                break;
            case NULL:
            default:
                break;
        }
    }

    private void populateDB() {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        ByteString byteString;
        switch (dbState) {
            case VALID:
                buffer.putLong(1);
                byteString = ByteString.copyFrom(new byte[0]);
                break;
            case INVALID:
                buffer.putLong(-1);
                byteString = ByteString.copyFrom(new byte[0]);
                break;
            case MASTER_KEY:
                buffer.putLong(1);
                byteString = ByteString.copyFrom("a".getBytes());
                break;
            case EMPTY:
            default:
                return;
        }
        LedgerData ledgerData = LedgerData.newBuilder().setExists(true).setFenced(false).setMasterKey(byteString)
                .build();
        map.put(buffer.array(), ledgerData.toByteArray());
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

        populateDB();

        this.sut = new LedgerMetadataIndex(new ServerConfiguration(), this.keyValueStorageFactory, "path",
                new NullStatsLogger());
    }

    @Test
    public void testLedgerMetadataIndex() {
        try {
            LedgerData result = sut.get(this.ledgerIdFirstGet);
            if (this.exceptionClass != null)
                Assert.fail("Exception not thrown");
            Assert.assertEquals(result, this.expected1);
        } catch (Exception e) {
            if (this.exceptionClass == null) {
                Assert.fail("Exception thrown");
                e.printStackTrace();
            }
            Assert.assertTrue(this.exceptionClass.isInstance(e));
            return;
        }
        try {
            executeChangeType();
            if (this.exceptionClassOnExecute != null)
                Assert.fail("Exception not thrown");
        } catch (Exception e) {
            if (this.exceptionClassOnExecute == null) {
                Assert.fail("Exception thrown");
                e.printStackTrace();
            }
            Assert.assertTrue(this.exceptionClassOnExecute.isInstance(e));
            return;
        }
        try {
            LedgerData result = sut.get(ledgerIdSecondGet);
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

    private enum DBState {
        EMPTY,
        VALID,
        INVALID,
        MASTER_KEY
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
