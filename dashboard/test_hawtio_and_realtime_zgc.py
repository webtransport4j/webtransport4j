import unittest
import urllib.request
import urllib.parse
import json

class TestHawtioAndRealtimeZgc(unittest.TestCase):

    def test_01_realtime_generational_zgc_introspection(self):
        """Verifies JVM 25 and Generational ZGC are strictly read from live JVM MXBeans, not hardcoded."""
        for port in [8081, 8082, 8083]:
            url = f"http://127.0.0.1:{port}/api/node/info"
            with urllib.request.urlopen(url, timeout=3.0) as resp:
                self.assertEqual(resp.status, 200)
                data = json.loads(resp.read().decode("utf-8"))

            jvm = data.get("jvm", {})
            self.assertEqual(jvm.get("feature"), 25, f"Expected Java 25 on port {port}")
            self.assertTrue(jvm.get("isGenerationalZgc"), f"Expected Generational ZGC on port {port}")
            self.assertEqual(jvm.get("gc"), "Generational ZGC")
            self.assertEqual(jvm.get("displayName"), "Java 25 · Generational ZGC")

            # Verify configuredGc from environment (defaults to ZGC)
            self.assertEqual(jvm.get("configuredGc"), "ZGC", f"Expected configuredGc=ZGC on port {port}")

            # Check that actual GC beans include ZGC minor and major cycles
            gc_beans = jvm.get("gcBeans", [])
            bean_names = [b.get("name") for b in gc_beans]
            self.assertTrue(any("Minor" in n for n in bean_names), f"Missing ZGC Minor bean: {bean_names}")
            self.assertTrue(any("Major" in n for n in bean_names), f"Missing ZGC Major bean: {bean_names}")

    def test_02_direct_jolokia_version_and_metadata(self):
        """Tests standard Jolokia 2.x REST protocol /jolokia/version on each node."""
        for port in [8081, 8082, 8083]:
            url = f"http://127.0.0.1:{port}/jolokia/version"
            with urllib.request.urlopen(url, timeout=3.0) as resp:
                self.assertEqual(resp.status, 200)
                data = json.loads(resp.read().decode("utf-8"))

            self.assertEqual(data.get("status"), 200)
            val = data.get("value", {})
            self.assertEqual(val.get("protocol"), "7.3")
            self.assertEqual(val.get("agent"), "2.1.2")
            self.assertEqual(val.get("info", {}).get("product"), "WebTransport4J")

    def test_03_direct_jolokia_mbean_read(self):
        """Tests reading JMX MBeans via Jolokia (java.lang:type=Memory and java.lang:type=Threading)."""
        url = "http://127.0.0.1:8081/jolokia/read/java.lang:type=Memory"
        with urllib.request.urlopen(url, timeout=3.0) as resp:
            self.assertEqual(resp.status, 200)
            data = json.loads(resp.read().decode("utf-8"))

        self.assertEqual(data.get("status"), 200)
        val = data.get("value", {})
        heap = val.get("HeapMemoryUsage", {})
        self.assertGreater(heap.get("used", 0), 0)
        self.assertGreater(heap.get("max", 0), 0)

        # Read specific attribute
        url_attr = "http://127.0.0.1:8081/jolokia/read/java.lang:type=Threading/ThreadCount"
        with urllib.request.urlopen(url_attr, timeout=3.0) as resp:
            self.assertEqual(resp.status, 200)
            attr_data = json.loads(resp.read().decode("utf-8"))
        self.assertGreater(attr_data.get("value", 0), 0)

    def test_04_direct_jolokia_threads_dump(self):
        """Tests live thread dump generation with stack traces via Jolokia."""
        url = "http://127.0.0.1:8081/jolokia/threads"
        with urllib.request.urlopen(url, timeout=3.0) as resp:
            self.assertEqual(resp.status, 200)
            data = json.loads(resp.read().decode("utf-8"))

        self.assertEqual(data.get("status"), 200)
        self.assertGreater(data.get("threadCount", 0), 0)
        threads = data.get("threads", [])
        self.assertGreater(len(threads), 0)

        # Check summary states
        summary = data.get("summary", {})
        self.assertIn("runnable", summary)
        self.assertIn("waiting", summary)
        self.assertIn("timedWaiting", summary)

    def test_05_direct_jolokia_mbeans_list(self):
        """Tests Jolokia MBean tree catalog listing for Hawtio tree view."""
        url = "http://127.0.0.1:8081/jolokia/list"
        with urllib.request.urlopen(url, timeout=4.0) as resp:
            self.assertEqual(resp.status, 200)
            data = json.loads(resp.read().decode("utf-8"))

        self.assertEqual(data.get("status"), 200)
        domains = data.get("value", {})
        self.assertIn("java.lang", domains)
        self.assertIn("java.nio", domains)

    def test_06_dashboard_hawtio_proxy_gateway(self):
        """Tests dashboard proxy forwarding /api/node/jolokia to target cluster nodes."""
        for port in [8081, 8082, 8083]:
            url = f"http://127.0.0.1:8085/api/node/jolokia/overview?port={port}"
            with urllib.request.urlopen(url, timeout=4.0) as resp:
                self.assertEqual(resp.status, 200)
                data = json.loads(resp.read().decode("utf-8"))

            self.assertEqual(data.get("status"), 200)
            jvm = data.get("jvm", {})
            self.assertEqual(jvm.get("displayName"), "Java 25 · Generational ZGC")
            mem = data.get("memory", {})
            self.assertGreater(mem.get("heapUsed", 0), 0)

    def test_07_cluster_status_and_live_telemetry_dynamic_agreement(self):
        """Verifies cluster status and live telemetry both dynamically reflect Java 25 · Generational ZGC."""
        with urllib.request.urlopen("http://127.0.0.1:8085/api/cluster/status", timeout=3.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            self.assertEqual(data.get("jvmEngine"), "Java 25 · Generational ZGC")
            # Verify env-based GC config is exposed
            self.assertEqual(data.get("configuredGc"), "ZGC")
            self.assertIn("-XX:+UseZGC", data.get("gcFlags", []))

        with urllib.request.urlopen("http://127.0.0.1:8085/api/live-telemetry", timeout=3.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            self.assertEqual(data.get("jvmGcType"), "Java 25 · Generational ZGC")

if __name__ == '__main__':
    unittest.main()
