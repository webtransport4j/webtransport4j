package io.github.webtransport4j.server;

import static org.junit.Assert.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.github.webtransport4j.api.WebTransportSession;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.junit.Test;

public class HmacQuicTokenHandlerTest {

  @Test
  public void testValidToken() throws Exception {
    HmacQuicTokenHandler handler = new HmacQuicTokenHandler();
    ByteBuf out = Unpooled.buffer();
    ByteBuf dcid = Unpooled.buffer();
    dcid.writeLong(12345L);
    
    InetSocketAddress address = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 4433);
    
    assertTrue(handler.writeToken(out, dcid, address));
    
    // Validate the token
    int result = handler.validateToken(out, address);
    assertTrue(result >= 0);
    assertEquals(40, result); // 40 bytes header (dcid starts at offset 40)
    
    out.release();
    dcid.release();
  }

  @Test
  public void testInvalidAddressToken() throws Exception {
    HmacQuicTokenHandler handler = new HmacQuicTokenHandler();
    ByteBuf out = Unpooled.buffer();
    ByteBuf dcid = Unpooled.buffer();
    
    InetSocketAddress address1 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 4433);
    InetSocketAddress address2 = new InetSocketAddress(InetAddress.getByName("192.168.1.1"), 4433);
    
    assertTrue(handler.writeToken(out, dcid, address1));
    
    // Validate with address2 - should fail
    int result = handler.validateToken(out, address2);
    assertEquals(-1, result);
    
    out.release();
    dcid.release();
  }

  @Test
  public void testExpiredToken() throws Exception {
    // 50ms expiration
    HmacQuicTokenHandler handler = new HmacQuicTokenHandler(new byte[32], 50L);
    ByteBuf out = Unpooled.buffer();
    ByteBuf dcid = Unpooled.buffer();
    InetSocketAddress address = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 4433);
    
    assertTrue(handler.writeToken(out, dcid, address));
    
    // Wait for it to expire
    Thread.sleep(100L);
    
    int result = handler.validateToken(out, address);
    assertEquals(-1, result);
    
    out.release();
    dcid.release();
  }

  @Test
  public void testTamperedSignatureToken() throws Exception {
    HmacQuicTokenHandler handler = new HmacQuicTokenHandler();
    ByteBuf out = Unpooled.buffer();
    ByteBuf dcid = Unpooled.buffer();
    InetSocketAddress address = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 4433);
    
    assertTrue(handler.writeToken(out, dcid, address));
    
    // Modify one byte of the signature (which starts at index 8)
    byte b = out.getByte(15);
    out.setByte(15, b ^ 1);
    
    int result = handler.validateToken(out, address);
    assertEquals(-1, result);
    
    out.release();
    dcid.release();
  }

  @Test
  public void testTamperedTimestampToken() throws Exception {
    HmacQuicTokenHandler handler = new HmacQuicTokenHandler();
    ByteBuf out = Unpooled.buffer();
    ByteBuf dcid = Unpooled.buffer();
    InetSocketAddress address = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 4433);
    
    assertTrue(handler.writeToken(out, dcid, address));
    
    // Modify one byte of the timestamp (starts at index 0)
    byte b = out.getByte(2);
    out.setByte(2, b ^ 1);
    
    int result = handler.validateToken(out, address);
    assertEquals(-1, result);
    
    out.release();
    dcid.release();
  }

  @Test
  public void testCustomExpirationConstructor() throws Exception {
    HmacQuicTokenHandler handler = new HmacQuicTokenHandler(50L);
    ByteBuf out = Unpooled.buffer();
    ByteBuf dcid = Unpooled.buffer();
    InetSocketAddress address = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 4433);
    
    assertTrue(handler.writeToken(out, dcid, address));
    
    int resultImmediate = handler.validateToken(out, address);
    assertTrue(resultImmediate >= 0);
    
    Thread.sleep(100L);
    
    out.readerIndex(0);
    int resultExpired = handler.validateToken(out, address);
    assertEquals(-1, resultExpired);
    
    out.release();
    dcid.release();
  }

  private interface ResumedSSLSession extends javax.net.ssl.SSLSession {
    boolean isSessionResumed();
  }

  @Test
  public void testSessionResumptionCheck() throws Exception {
    QuicStreamChannel mockConnectStream = org.mockito.Mockito.mock(QuicStreamChannel.class);
    QuicChannel mockQuicChannel = org.mockito.Mockito.mock(QuicChannel.class);
    javax.net.ssl.SSLEngine mockSslEngine = org.mockito.Mockito.mock(javax.net.ssl.SSLEngine.class);
    ResumedSSLSession mockSession = org.mockito.Mockito.mock(ResumedSSLSession.class);
    
    org.mockito.Mockito.when(mockConnectStream.parent()).thenReturn(mockQuicChannel);
    org.mockito.Mockito.when(mockQuicChannel.sslEngine()).thenReturn(mockSslEngine);
    org.mockito.Mockito.when(mockSslEngine.getSession()).thenReturn(mockSession);
    
    // Case 1: Session is resumed
    org.mockito.Mockito.when(mockSession.isSessionResumed()).thenReturn(true);
    WebTransportSession wtSession = new WebTransportSession(
        1L, mockConnectStream, "/test", 10, 10, 1000, 10, 10, 1000, false
    );
    assertTrue(wtSession.isSessionResumed());
    
    // Case 2: Session is NOT resumed
    org.mockito.Mockito.when(mockSession.isSessionResumed()).thenReturn(false);
    assertFalse(wtSession.isSessionResumed());
  }
}
