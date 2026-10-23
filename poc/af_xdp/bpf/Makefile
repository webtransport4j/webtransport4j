# Makefile for WebTransport AF_XDP eBPF Program
CLANG ?= clang
LLVM_STRIP ?= llvm-strip
BPFTOOL ?= bpftool

TARGET := xdp_webtransport.bpf.o
SOURCE := xdp_webtransport.bpf.c

ARCH := $(shell uname -m | sed 's/x86_64/x86/' | sed 's/aarch64/arm64/')

CFLAGS := -g -O2 -target bpf -D__TARGET_ARCH_$(ARCH) \
          -Wall -Werror \
          -I/usr/include/$(shell uname -m)-linux-gnu \
          -I/usr/include

all: $(TARGET)

$(TARGET): $(SOURCE)
	$(CLANG) $(CFLAGS) -c $< -o $@
	$(LLVM_STRIP) -g $@ 2>/dev/null || true
	@echo "Successfully compiled eBPF filter: $(TARGET)"

clean:
	rm -f $(TARGET)

# Attach to an interface (e.g., make load IFACE=eth0)
load: $(TARGET)
	@if [ -z "$(IFACE)" ]; then echo "Usage: make load IFACE=<interface>"; exit 1; fi
	ip link set dev $(IFACE) xdpgeneric obj $(TARGET) sec xdp
	@echo "Attached $(TARGET) to $(IFACE)"

# Detach from an interface (e.g., make unload IFACE=eth0)
unload:
	@if [ -z "$(IFACE)" ]; then echo "Usage: make unload IFACE=<interface>"; exit 1; fi
	ip link set dev $(IFACE) xdpgeneric off
	@echo "Detached XDP program from $(IFACE)"

.PHONY: all clean load unload
