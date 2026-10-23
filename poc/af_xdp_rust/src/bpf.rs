//! eBPF and Aya Integration for AF_XDP WebTransport
//!
//! Handles loading the compiled XDP BPF program into the Linux kernel
//! and managing the XSK socket map (`xsks_map`) via Aya.

use aya::{
    Ebpf,
    maps::xdp::XskMap,
    programs::xdp::{Xdp, XdpLinkId, XdpMode},
};
use std::os::fd::AsRawFd;

pub struct BpfManager {
    ebpf: Ebpf,
}

impl BpfManager {
    /// Load eBPF bytecode into the kernel using Aya.
    pub fn load(bpf_bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let ebpf = Ebpf::load(bpf_bytes)?;
        Ok(Self { ebpf })
    }

    /// Attach the XDP program to the specified network interface (e.g., "eth0").
    /// Uses SKB mode (Generic XDP) for Wi-Fi and interfaces without native driver XDP support.
    pub fn attach_xdp(
        &mut self,
        iface: &str,
        skb_mode: bool,
    ) -> Result<XdpLinkId, Box<dyn std::error::Error>> {
        let program: &mut Xdp = self
            .ebpf
            .program_mut("xdp_webtransport")
            .ok_or("Program 'xdp_webtransport' not found in BPF object")?
            .try_into()?;

        program.load()?;

        let mode = if skb_mode {
            XdpMode::Skb
        } else {
            XdpMode::Driver
        };

        let link_id = program.attach(iface, mode)?;
        Ok(link_id)
    }

    /// Register an AF_XDP socket file descriptor into the `xsks_map` at the specified queue.
    pub fn register_socket(
        &mut self,
        queue_id: u32,
        socket_fd: impl AsRawFd,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut xsk_map: XskMap<_> = self
            .ebpf
            .take_map("xsks_map")
            .ok_or("Map 'xsks_map' not found in BPF object")?
            .try_into()?;

        xsk_map.set(queue_id, socket_fd.as_raw_fd(), 0)?;
        Ok(())
    }
}
