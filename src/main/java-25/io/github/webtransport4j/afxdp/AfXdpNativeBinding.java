package io.github.webtransport4j.afxdp;

import java.io.File;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Native binding to Linux {@code libxdp} and standard C runtime via Java 25 Foreign Function and Memory (FFM).
 *
 * <p>Provides safe downcalls for UMEM registration and XSK socket creation, abstracting kernel driver
 * and eBPF dispatcher lifecycle management.
 */
public final class AfXdpNativeBinding {

  private static final Linker LINKER = Linker.nativeLinker();
  private static final StructLayout CAPTURED_STATE_LAYOUT = Linker.Option.captureStateLayout();
  private static final VarHandle ERRNO_HANDLE =
      CAPTURED_STATE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("errno"));

  private static final boolean AVAILABLE;
  private static final String LOAD_ERROR;

  private static MethodHandle MH_XSK_UMEM_CREATE;
  private static MethodHandle MH_XSK_SOCKET_CREATE;
  private static MethodHandle MH_XSK_SOCKET_DELETE;
  private static MethodHandle MH_XSK_UMEM_DELETE;
  private static MethodHandle MH_XSK_SOCKET_FD;
  private static MethodHandle MH_SENDTO;

  static {
    boolean available = false;
    String error = null;

    String os = System.getProperty("os.name", "").toLowerCase();
    if (!os.contains("linux")) {
      error = "AF_XDP native transport is only supported on Linux (found: " + os + ")";
    } else {
      try {
        SymbolLookup libxdp = findLibXdp();
        if (libxdp == null) {
          error = "libxdp.so.1 / libxdp.so not found in system library paths";
        } else {
          MH_XSK_UMEM_CREATE = LINKER.downcallHandle(
              libxdp.find("xsk_umem__create").orElseThrow(),
              FunctionDescriptor.of(
                  ValueLayout.JAVA_INT,
                  ValueLayout.ADDRESS,
                  ValueLayout.ADDRESS,
                  ValueLayout.JAVA_LONG,
                  ValueLayout.ADDRESS,
                  ValueLayout.ADDRESS,
                  ValueLayout.ADDRESS
              ),
              Linker.Option.captureCallState("errno")
          );

          MH_XSK_SOCKET_CREATE = LINKER.downcallHandle(
              libxdp.find("xsk_socket__create").orElseThrow(),
              FunctionDescriptor.of(
                  ValueLayout.JAVA_INT,
                  ValueLayout.ADDRESS,
                  ValueLayout.ADDRESS,
                  ValueLayout.JAVA_INT,
                  ValueLayout.ADDRESS,
                  ValueLayout.ADDRESS,
                  ValueLayout.ADDRESS,
                  ValueLayout.ADDRESS
              ),
              Linker.Option.captureCallState("errno")
          );

          MH_XSK_SOCKET_DELETE = LINKER.downcallHandle(
              libxdp.find("xsk_socket__delete").orElseThrow(),
              FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
          );

          MH_XSK_UMEM_DELETE = LINKER.downcallHandle(
              libxdp.find("xsk_umem__delete").orElseThrow(),
              FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
          );

          MH_XSK_SOCKET_FD = LINKER.downcallHandle(
              libxdp.find("xsk_socket__fd").orElseThrow(),
              FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS)
          );

          Optional<MemorySegment> sendtoAddr = LINKER.defaultLookup().find("sendto");
          if (sendtoAddr.isPresent()) {
            MH_SENDTO = LINKER.downcallHandle(
                sendtoAddr.get(),
                FunctionDescriptor.of(
                    ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_INT,
                    ValueLayout.ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_INT,
                    ValueLayout.ADDRESS,
                    ValueLayout.JAVA_INT
                )
            );
          }

          available = true;
        }
      } catch (Throwable t) {
        error = "Failed to initialize libxdp FFM bindings: " + t.getMessage();
      }
    }

    AVAILABLE = available;
    LOAD_ERROR = error;
  }

  private AfXdpNativeBinding() {}

  /**
   * Returns true if {@code libxdp} was located and successfully linked.
   */
  public static boolean isAvailable() {
    return AVAILABLE;
  }

  /**
   * Returns details of why native AF_XDP binding is unavailable, or null if available.
   */
  public static String getLoadError() {
    return LOAD_ERROR;
  }

  private static SymbolLookup findLibXdp() {
    String[] candidateNames = new String[] {
        "libxdp.so.1",
        "libxdp.so",
        "/usr/lib/x86_64-linux-gnu/libxdp.so.1",
        "/usr/lib64/libxdp.so.1",
        "/usr/local/lib/libxdp.so.1",
        "/usr/lib/aarch64-linux-gnu/libxdp.so.1"
    };

    for (String name : candidateNames) {
      try {
        if (name.startsWith("/") && !new File(name).exists()) {
          continue;
        }
        return SymbolLookup.libraryLookup(name, Arena.global());
      } catch (Throwable ignored) {
        // try next candidate
      }
    }
    return null;
  }

  /**
   * Opens and binds an AF_XDP native socket using {@code libxdp}.
   *
   * @param ifname interface name (e.g., "eth0", "lo")
   * @param queueId hardware RX/TX queue index
   * @param umem pre-allocated off-heap UMEM buffer pool
   * @param ringSize number of descriptors per ring (must be power of two, e.g., 2048)
   * @return initialized native socket
   * @throws AfXdpException if kernel registration or binding fails
   */
  public static AfXdpNativeSocket openSocket(
      String ifname, int queueId, AfXdpUmem umem, int ringSize) throws AfXdpException {
    if (!AVAILABLE) {
      throw new AfXdpException("AF_XDP native transport unavailable: " + LOAD_ERROR);
    }

    Arena socketArena = Arena.ofShared();
    try {
      MemorySegment umemPtr = socketArena.allocate(ValueLayout.ADDRESS);
      MemorySegment fillRingStruct = socketArena.allocate(AfXdpRing.RING_STRUCT_SIZE, 8);
      MemorySegment compRingStruct = socketArena.allocate(AfXdpRing.RING_STRUCT_SIZE, 8);

      MemorySegment umemConfig = socketArena.allocate(20, 4);
      umemConfig.set(ValueLayout.JAVA_INT, 0, ringSize); // fill_size
      umemConfig.set(ValueLayout.JAVA_INT, 4, ringSize); // comp_size
      umemConfig.set(ValueLayout.JAVA_INT, 8, umem.getFrameSize()); // frame_size
      umemConfig.set(ValueLayout.JAVA_INT, 12, 0); // frame_headroom
      umemConfig.set(ValueLayout.JAVA_INT, 16, 0); // flags

      MemorySegment capturedState = socketArena.allocate(CAPTURED_STATE_LAYOUT);
      long totalUmemBytes = (long) umem.getFrameSize() * umem.getFrameCount();

      int umemRes = (int) MH_XSK_UMEM_CREATE.invokeExact(
          capturedState,
          umemPtr,
          MemorySegment.ofAddress(umem.getBaseAddress()),
          totalUmemBytes,
          fillRingStruct,
          compRingStruct,
          umemConfig
      );

      if (umemRes != 0) {
        int errno = (int) ERRNO_HANDLE.get(capturedState, 0L);
        throw new AfXdpException(
            "xsk_umem__create failed for " + ifname + " (errno " + errno + ": " + describeErrno(errno) + ")",
            errno
        );
      }

      MemorySegment umemHandle = umemPtr.get(ValueLayout.ADDRESS, 0);

      MemorySegment xskPtr = socketArena.allocate(ValueLayout.ADDRESS);
      MemorySegment rxRingStruct = socketArena.allocate(AfXdpRing.RING_STRUCT_SIZE, 8);
      MemorySegment txRingStruct = socketArena.allocate(AfXdpRing.RING_STRUCT_SIZE, 8);

      MemorySegment socketConfig = socketArena.allocate(20, 4);
      socketConfig.set(ValueLayout.JAVA_INT, 0, ringSize); // rx_size
      socketConfig.set(ValueLayout.JAVA_INT, 4, ringSize); // tx_size
      socketConfig.set(ValueLayout.JAVA_INT, 8, 0); // libxdp_flags
      socketConfig.set(ValueLayout.JAVA_INT, 12, 0); // xdp_flags
      socketConfig.set(ValueLayout.JAVA_SHORT, 16, (short) 0); // bind_flags

      byte[] ifnameBytes = ifname.getBytes(StandardCharsets.US_ASCII);
      MemorySegment cIfname = socketArena.allocate(ifnameBytes.length + 1);
      cIfname.copyFrom(MemorySegment.ofArray(ifnameBytes));
      cIfname.set(ValueLayout.JAVA_BYTE, ifnameBytes.length, (byte) 0);

      int xskRes = (int) MH_XSK_SOCKET_CREATE.invokeExact(
          capturedState,
          xskPtr,
          cIfname,
          queueId,
          umemHandle,
          rxRingStruct,
          txRingStruct,
          socketConfig
      );

      if (xskRes != 0) {
        int errno = (int) ERRNO_HANDLE.get(capturedState, 0L);
        MH_XSK_UMEM_DELETE.invokeExact(umemHandle);
        throw new AfXdpException(
            "xsk_socket__create failed for " + ifname + " queue " + queueId
                + " (errno " + errno + ": " + describeErrno(errno) + ")",
            errno
        );
      }

      MemorySegment xskHandle = xskPtr.get(ValueLayout.ADDRESS, 0);
      int fd = (int) MH_XSK_SOCKET_FD.invokeExact(xskHandle);

      AfXdpRing fillRing = new AfXdpRing(fillRingStruct, AfXdpRing.ADDR_ENTRY_SIZE);
      AfXdpRing compRing = new AfXdpRing(compRingStruct, AfXdpRing.ADDR_ENTRY_SIZE);
      AfXdpRing rxRing = new AfXdpRing(rxRingStruct, AfXdpRing.XDP_DESC_SIZE);
      AfXdpRing txRing = new AfXdpRing(txRingStruct, AfXdpRing.XDP_DESC_SIZE);

      return new AfXdpNativeSocket(
          socketArena,
          umemHandle,
          xskHandle,
          fd,
          ifname,
          queueId,
          rxRing,
          txRing,
          fillRing,
          compRing,
          umem
      );
    } catch (AfXdpException e) {
      socketArena.close();
      throw e;
    } catch (Throwable t) {
      socketArena.close();
      throw new AfXdpException("Unexpected error during AF_XDP socket initialization: " + t.getMessage(), t);
    }
  }

  static void deleteSocket(MemorySegment xskHandle) {
    if (MH_XSK_SOCKET_DELETE != null && xskHandle != null && xskHandle.address() != 0) {
      try {
        MH_XSK_SOCKET_DELETE.invokeExact(xskHandle);
      } catch (Throwable ignored) {
        // ignore on cleanup
      }
    }
  }

  static void deleteUmem(MemorySegment umemHandle) {
    if (MH_XSK_UMEM_DELETE != null && umemHandle != null && umemHandle.address() != 0) {
      try {
        MH_XSK_UMEM_DELETE.invokeExact(umemHandle);
      } catch (Throwable ignored) {
        // ignore on cleanup
      }
    }
  }

  static void kickTx(int fd) {
    if (MH_SENDTO != null && fd >= 0) {
      try {
        MH_SENDTO.invokeExact(fd, MemorySegment.NULL, 0L, 0x40 /* MSG_DONTWAIT */, MemorySegment.NULL, 0);
      } catch (Throwable ignored) {
        // ignore kick failures
      }
    }
  }

  /**
   * Translates Linux errnos into clear operational descriptions.
   */
  public static String describeErrno(int errno) {
    return switch (errno) {
      case 1 -> "EPERM (Operation not permitted - requires CAP_NET_ADMIN / CAP_BPF capabilities or root)";
      case 13 -> "EACCES (Permission denied)";
      case 19 -> "ENODEV (No such network interface)";
      case 22 -> "EINVAL (Invalid argument or unsupported frame size)";
      case 95 -> "EOPNOTSUPP (Operation not supported by NIC driver or XDP mode)";
      case 105 -> "ENOBUFS (No buffer space available in ring)";
      default -> "Linux errno " + errno;
    };
  }
}
