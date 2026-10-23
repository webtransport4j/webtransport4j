// SPDX-License-Identifier: GPL-2.0
/*
 * eBPF XDP Filter for WebTransport Kernel Bypass (IPv4 and IPv6)
 *
 * Intercepts incoming UDP packets on the WebTransport port (default 443)
 * directly at the network interface driver level and redirects them
 * to the AF_XDP (XSK) socket map without kernel network stack traversal.
 */

#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/ipv6.h>
#include <linux/in.h>
#include <linux/udp.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

#ifndef WEBTRANSPORT_DEFAULT_PORT
#define WEBTRANSPORT_DEFAULT_PORT 443
#endif

/*
 * XSK map storing active AF_XDP socket file descriptors indexed
 * by the network interface queue ID.
 */
struct {
    __uint(type, BPF_MAP_TYPE_XSKMAP);
    __uint(max_entries, 64);
    __type(key, __u32);
    __type(value, __u32);
} xsks_map SEC(".maps");

SEC("xdp")
int xdp_webtransport_router(struct xdp_md *ctx) {
    void *data = (void *)(long)ctx->data;
    void *data_end = (void *)(long)ctx->data_end;

    // 1. Validate Ethernet frame header
    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end) {
        return XDP_PASS;
    }

    // 2. Handle IPv4 traffic
    if (eth->h_proto == bpf_htons(ETH_P_IP)) {
        struct iphdr *ip = (void *)(eth + 1);
        if ((void *)(ip + 1) > data_end) {
            return XDP_PASS;
        }
        if (ip->protocol != IPPROTO_UDP) {
            return XDP_PASS;
        }
        __u32 ip_hdr_len = ip->ihl * 4;
        if (ip_hdr_len < sizeof(struct iphdr)) {
            return XDP_PASS;
        }
        struct udphdr *udp = (void *)((char *)ip + ip_hdr_len);
        if ((void *)(udp + 1) > data_end) {
            return XDP_PASS;
        }
        if (udp->dest == bpf_htons(WEBTRANSPORT_DEFAULT_PORT)) {
            return bpf_redirect_map(&xsks_map, ctx->rx_queue_index, XDP_PASS);
        }
    }

    // 3. Handle IPv6 traffic
    if (eth->h_proto == bpf_htons(ETH_P_IPV6)) {
        struct ipv6hdr *ip6 = (void *)(eth + 1);
        if ((void *)(ip6 + 1) > data_end) {
            return XDP_PASS;
        }
        if (ip6->nexthdr != IPPROTO_UDP) {
            return XDP_PASS;
        }
        struct udphdr *udp = (void *)(ip6 + 1);
        if ((void *)(udp + 1) > data_end) {
            return XDP_PASS;
        }
        if (udp->dest == bpf_htons(WEBTRANSPORT_DEFAULT_PORT)) {
            return bpf_redirect_map(&xsks_map, ctx->rx_queue_index, XDP_PASS);
        }
    }

    // Fallback: Non-WebTransport traffic passes directly to standard OS network stack
    return XDP_PASS;
}

char _license[] SEC("license") = "GPL";
