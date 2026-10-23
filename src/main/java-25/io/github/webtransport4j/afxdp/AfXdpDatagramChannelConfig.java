package io.github.webtransport4j.afxdp;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultChannelConfig;
import io.netty.channel.MessageSizeEstimator;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramChannelConfig;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Map;

/**
 * Configuration for an {@link AfXdpDatagramChannel}.
 */
public final class AfXdpDatagramChannelConfig extends DefaultChannelConfig
    implements DatagramChannelConfig {

  private volatile boolean broadcast;
  private volatile int timeToLive = 64;
  private volatile int receiveBufferSize = 1024 * 1024;
  private volatile int sendBufferSize = 1024 * 1024;
  private volatile int trafficClass;
  private volatile boolean reuseAddress = true;
  private volatile boolean loopbackModeDisabled;
  private volatile NetworkInterface networkInterface;
  private volatile InetAddress networkInterfaceAddress;
  private volatile String interfaceName;
  private volatile int queueId;
  private volatile byte[] sourceMac;
  private volatile byte[] gatewayMac;

  public AfXdpDatagramChannelConfig(DatagramChannel channel) {
    super(channel);
  }

  @Override
  public Map<ChannelOption<?>, Object> getOptions() {
    return getOptions(
        super.getOptions(),
        ChannelOption.SO_BROADCAST,
        ChannelOption.SO_RCVBUF,
        ChannelOption.SO_SNDBUF,
        ChannelOption.SO_REUSEADDR,
        ChannelOption.IP_MULTICAST_TTL,
        ChannelOption.IP_MULTICAST_LOOP_DISABLED,
        ChannelOption.IP_TOS,
        AfXdpChannelOption.INTERFACE_NAME,
        AfXdpChannelOption.QUEUE_ID,
        AfXdpChannelOption.SOURCE_MAC,
        AfXdpChannelOption.GATEWAY_MAC,
        AfXdpChannelOption.NATIVE_MODE_REQUIRED);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getOption(ChannelOption<T> option) {

    if (option == ChannelOption.SO_BROADCAST) {
      return (T) Boolean.valueOf(isBroadcast());
    }
    if (option == ChannelOption.SO_RCVBUF) {
      return (T) Integer.valueOf(getReceiveBufferSize());
    }
    if (option == ChannelOption.SO_SNDBUF) {
      return (T) Integer.valueOf(getSendBufferSize());
    }
    if (option == ChannelOption.SO_REUSEADDR) {
      return (T) Boolean.valueOf(isReuseAddress());
    }
    if (option == ChannelOption.IP_MULTICAST_TTL) {
      return (T) Integer.valueOf(getTimeToLive());
    }
    if (option == ChannelOption.IP_MULTICAST_LOOP_DISABLED) {
      return (T) Boolean.valueOf(isLoopbackModeDisabled());
    }
    if (option == ChannelOption.IP_TOS) {
      return (T) Integer.valueOf(getTrafficClass());
    }
    if (option == AfXdpChannelOption.INTERFACE_NAME) {
      return (T) getInterfaceName();
    }
    if (option == AfXdpChannelOption.QUEUE_ID) {
      return (T) Integer.valueOf(getQueueId());
    }
    if (option == AfXdpChannelOption.SOURCE_MAC) {
      return (T) getSourceMac();
    }
    if (option == AfXdpChannelOption.GATEWAY_MAC) {
      return (T) getGatewayMac();
    }
    if (option == AfXdpChannelOption.NATIVE_MODE_REQUIRED) {
      return (T) Boolean.valueOf(isNativeModeRequired());
    }
    return super.getOption(option);
  }

  @Override
  public <T> boolean setOption(ChannelOption<T> option, T value) {
    validate(option, value);
    if (option == ChannelOption.SO_BROADCAST) {
      setBroadcast((Boolean) value);
    } else if (option == ChannelOption.SO_RCVBUF) {
      setReceiveBufferSize((Integer) value);
    } else if (option == ChannelOption.SO_SNDBUF) {
      setSendBufferSize((Integer) value);
    } else if (option == ChannelOption.SO_REUSEADDR) {
      setReuseAddress((Boolean) value);
    } else if (option == ChannelOption.IP_MULTICAST_TTL) {
      setTimeToLive((Integer) value);
    } else if (option == ChannelOption.IP_MULTICAST_LOOP_DISABLED) {
      setLoopbackModeDisabled((Boolean) value);
    } else if (option == ChannelOption.IP_TOS) {
      setTrafficClass((Integer) value);
    } else if (option == AfXdpChannelOption.INTERFACE_NAME) {
      setInterfaceName((String) value);
    } else if (option == AfXdpChannelOption.QUEUE_ID) {
      setQueueId((Integer) value);
    } else if (option == AfXdpChannelOption.SOURCE_MAC) {
      setSourceMac((byte[]) value);
    } else if (option == AfXdpChannelOption.GATEWAY_MAC) {
      setGatewayMac((byte[]) value);
    } else if (option == AfXdpChannelOption.NATIVE_MODE_REQUIRED) {
      setNativeModeRequired((Boolean) value);
    } else {
      return super.setOption(option, value);
    }
    return true;
  }


  @Override
  public boolean isBroadcast() {
    return broadcast;
  }

  @Override
  public DatagramChannelConfig setBroadcast(boolean broadcast) {
    this.broadcast = broadcast;
    return this;
  }

  @Override
  public boolean isLoopbackModeDisabled() {
    return loopbackModeDisabled;
  }

  @Override
  public DatagramChannelConfig setLoopbackModeDisabled(boolean loopbackModeDisabled) {
    this.loopbackModeDisabled = loopbackModeDisabled;
    return this;
  }

  @Override
  @Deprecated
  @SuppressWarnings("deprecation")
  public int getTimeToLive() {
    return timeToLive;
  }

  @Override
  @Deprecated
  @SuppressWarnings("deprecation")
  public DatagramChannelConfig setTimeToLive(int ttl) {
    this.timeToLive = ttl;
    return this;
  }

  @Override
  @Deprecated
  @SuppressWarnings("deprecation")
  public InetAddress getInterface() {
    return networkInterfaceAddress;
  }

  @Override
  @Deprecated
  @SuppressWarnings("deprecation")
  public DatagramChannelConfig setInterface(InetAddress interfaceAddress) {
    this.networkInterfaceAddress = interfaceAddress;
    return this;
  }

  @Override
  public NetworkInterface getNetworkInterface() {
    return networkInterface;
  }

  @Override
  public DatagramChannelConfig setNetworkInterface(NetworkInterface networkInterface) {
    this.networkInterface = networkInterface;
    return this;
  }

  @Override
  public boolean isReuseAddress() {
    return reuseAddress;
  }

  @Override
  public DatagramChannelConfig setReuseAddress(boolean reuseAddress) {
    this.reuseAddress = reuseAddress;
    return this;
  }

  @Override
  public int getReceiveBufferSize() {
    return receiveBufferSize;
  }

  @Override
  public DatagramChannelConfig setReceiveBufferSize(int receiveBufferSize) {
    this.receiveBufferSize = receiveBufferSize;
    return this;
  }

  @Override
  public int getSendBufferSize() {
    return sendBufferSize;
  }

  @Override
  public DatagramChannelConfig setSendBufferSize(int sendBufferSize) {
    this.sendBufferSize = sendBufferSize;
    return this;
  }

  @Override
  public int getTrafficClass() {
    return trafficClass;
  }

  @Override
  public DatagramChannelConfig setTrafficClass(int trafficClass) {
    this.trafficClass = trafficClass;
    return this;
  }

  @Override
  public DatagramChannelConfig setWriteSpinCount(int writeSpinCount) {
    super.setWriteSpinCount(writeSpinCount);
    return this;
  }

  @Override
  public DatagramChannelConfig setConnectTimeoutMillis(int connectTimeoutMillis) {
    super.setConnectTimeoutMillis(connectTimeoutMillis);
    return this;
  }

  @Override
  public DatagramChannelConfig setMaxMessagesPerRead(int maxMessagesPerRead) {
    super.setMaxMessagesPerRead(maxMessagesPerRead);
    return this;
  }

  @Override
  public DatagramChannelConfig setAllocator(ByteBufAllocator allocator) {
    super.setAllocator(allocator);
    return this;
  }

  @Override
  public DatagramChannelConfig setRecvByteBufAllocator(RecvByteBufAllocator allocator) {
    super.setRecvByteBufAllocator(allocator);
    return this;
  }

  @Override
  public DatagramChannelConfig setAutoRead(boolean autoRead) {
    super.setAutoRead(autoRead);
    return this;
  }

  @Override
  public DatagramChannelConfig setAutoClose(boolean autoClose) {
    super.setAutoClose(autoClose);
    return this;
  }

  @Override
  public DatagramChannelConfig setWriteBufferHighWaterMark(int writeBufferHighWaterMark) {
    super.setWriteBufferHighWaterMark(writeBufferHighWaterMark);
    return this;
  }

  @Override
  public DatagramChannelConfig setWriteBufferLowWaterMark(int writeBufferLowWaterMark) {
    super.setWriteBufferLowWaterMark(writeBufferLowWaterMark);
    return this;
  }

  @Override
  public DatagramChannelConfig setWriteBufferWaterMark(WriteBufferWaterMark writeBufferWaterMark) {
    super.setWriteBufferWaterMark(writeBufferWaterMark);
    return this;
  }

  @Override
  public DatagramChannelConfig setMessageSizeEstimator(MessageSizeEstimator estimator) {
    super.setMessageSizeEstimator(estimator);
    return this;
  }

  @Override
  public DatagramChannelConfig setMaxMessagesPerWrite(int maxMessagesPerWrite) {
    super.setMaxMessagesPerWrite(maxMessagesPerWrite);
    return this;
  }

  /**
   * Returns the network interface name assigned to this AF_XDP channel.
   *
   * @return network interface name, or null if unset
   */
  public String getInterfaceName() {
    return interfaceName;
  }

  /**
   * Sets the network interface name to bind for AF_XDP.
   *
   * @param interfaceName interface name (e.g. "eth0")
   * @return this config
   */
  public AfXdpDatagramChannelConfig setInterfaceName(String interfaceName) {
    this.interfaceName = interfaceName;
    return this;
  }

  /**
   * Returns the NIC hardware queue ID.
   *
   * @return queue ID
   */
  public int getQueueId() {
    return queueId;
  }

  /**
   * Sets the NIC hardware queue ID to bind.
   *
   * @param queueId hardware queue ID
   * @return this config
   */
  public AfXdpDatagramChannelConfig setQueueId(int queueId) {
    this.queueId = queueId;
    return this;
  }

  /**
   * Returns the local source MAC address.
   *
   * @return 6-byte source MAC address, or null if unset
   */
  public byte[] getSourceMac() {
    return sourceMac != null ? sourceMac.clone() : null;
  }

  /**
   * Sets the local source MAC address for outbound Layer 2 Ethernet frames.
   *
   * @param sourceMac 6-byte MAC address
   * @return this config
   */
  public AfXdpDatagramChannelConfig setSourceMac(byte[] sourceMac) {
    this.sourceMac = sourceMac != null ? sourceMac.clone() : null;
    return this;
  }

  /**
   * Returns the default gateway MAC address.
   *
   * @return 6-byte gateway MAC address, or null if unset
   */
  public byte[] getGatewayMac() {
    return gatewayMac != null ? gatewayMac.clone() : null;
  }

  private volatile boolean nativeModeRequired;

  /**
   * Sets the default gateway MAC address for outbound packets without learned MAC.
   *
   * @param gatewayMac 6-byte MAC address
   * @return this config
   */
  public AfXdpDatagramChannelConfig setGatewayMac(byte[] gatewayMac) {
    this.gatewayMac = gatewayMac != null ? gatewayMac.clone() : null;
    return this;
  }

  /**
   * Returns true if channel binding must fail when native AF_XDP is unavailable.
   *
   * @return true if native mode is required
   */
  public boolean isNativeModeRequired() {
    return nativeModeRequired;
  }

  /**
   * Configures whether channel binding must fail when native AF_XDP is unavailable.
   *
   * @param nativeModeRequired true to require native AF_XDP
   * @return this config
   */
  public AfXdpDatagramChannelConfig setNativeModeRequired(boolean nativeModeRequired) {
    this.nativeModeRequired = nativeModeRequired;
    return this;
  }
}

