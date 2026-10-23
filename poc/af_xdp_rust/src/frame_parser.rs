//! Zero-copy packet parser for AF_XDP UMEM frames.
//! Supports dual-stack IPv4 (0x0800) and IPv6 (0x86DD) over Ethernet II framing.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpProtocol {
    Ipv4,
    Ipv6,
}

#[derive(Debug)]
pub struct ParsedPacket<'a> {
    pub protocol: IpProtocol,
    pub src_port: u16,
    pub dst_port: u16,
    pub payload: &'a [u8],
}

pub struct AfXdpFrameParser;

impl AfXdpFrameParser {
    pub const ETH_HDR_LEN: usize = 14;
    pub const ETH_P_IP: u16 = 0x0800;
    pub const ETH_P_IPV6: u16 = 0x86DD;
    pub const IPPROTO_UDP: u8 = 17;

    /// Parse an Ethernet II frame zero-copy from an AF_XDP UMEM frame slice.
    #[inline(always)]
    pub fn parse(frame: &[u8]) -> Option<ParsedPacket<'_>> {
        if frame.len() < Self::ETH_HDR_LEN {
            return None;
        }

        let ether_type = u16::from_be_bytes([frame[12], frame[13]]);
        match ether_type {
            Self::ETH_P_IP => Self::parse_ipv4(frame),
            Self::ETH_P_IPV6 => Self::parse_ipv6(frame),
            _ => None,
        }
    }

    #[inline(always)]
    fn parse_ipv4(frame: &[u8]) -> Option<ParsedPacket<'_>> {
        let ip_offset = Self::ETH_HDR_LEN;
        if frame.len() < ip_offset + 20 {
            return None;
        }

        let ver_ihl = frame[ip_offset];
        let ihl = (ver_ihl & 0x0F) as usize * 4;
        let protocol = frame[ip_offset + 9];

        if protocol != Self::IPPROTO_UDP || frame.len() < ip_offset + ihl + 8 {
            return None;
        }

        let udp_offset = ip_offset + ihl;
        let src_port = u16::from_be_bytes([frame[udp_offset], frame[udp_offset + 1]]);
        let dst_port = u16::from_be_bytes([frame[udp_offset + 2], frame[udp_offset + 3]]);
        let udp_len = u16::from_be_bytes([frame[udp_offset + 4], frame[udp_offset + 5]]) as usize;

        if udp_len < 8 || frame.len() < udp_offset + udp_len {
            return None;
        }

        let payload = &frame[udp_offset + 8..udp_offset + udp_len];
        Some(ParsedPacket {
            protocol: IpProtocol::Ipv4,
            src_port,
            dst_port,
            payload,
        })
    }

    #[inline(always)]
    fn parse_ipv6(frame: &[u8]) -> Option<ParsedPacket<'_>> {
        let ip_offset = Self::ETH_HDR_LEN;
        // IPv6 header is fixed 40 bytes
        if frame.len() < ip_offset + 40 {
            return None;
        }

        let next_header = frame[ip_offset + 6];
        let _payload_len = u16::from_be_bytes([frame[ip_offset + 4], frame[ip_offset + 5]]) as usize;

        if next_header != Self::IPPROTO_UDP || frame.len() < ip_offset + 40 + 8 {
            return None;
        }

        let udp_offset = ip_offset + 40;
        let src_port = u16::from_be_bytes([frame[udp_offset], frame[udp_offset + 1]]);
        let dst_port = u16::from_be_bytes([frame[udp_offset + 2], frame[udp_offset + 3]]);
        let udp_len = u16::from_be_bytes([frame[udp_offset + 4], frame[udp_offset + 5]]) as usize;

        if udp_len < 8 || frame.len() < udp_offset + udp_len {
            return None;
        }

        let payload = &frame[udp_offset + 8..udp_offset + udp_len];
        Some(ParsedPacket {
            protocol: IpProtocol::Ipv6,
            src_port,
            dst_port,
            payload,
        })
    }

    /// Fast XOR checksum of the payload to simulate application layer parsing
    #[inline(always)]
    pub fn xor_checksum(payload: &[u8]) -> u64 {
        let mut xor: u64 = 0;
        let chunks = payload.chunks_exact(8);
        let remainder = chunks.remainder();

        for chunk in chunks {
            xor ^= u64::from_ne_bytes(chunk.try_into().unwrap());
        }

        for &b in remainder {
            xor ^= b as u64;
        }

        xor
    }
}
