# webtransport4j-incubator

The first high-performance WebTransport server for the Java ecosystem, powered by Netty's asynchronous HTTP/3 stack.

# Local Development Guide

Follow these steps to run `webtransport4j` locally with a trusted self-signed certificate and a secure browser connection.

## 1. Generate Certificates (mkcert)

WebTransport requires HTTPS. We use `mkcert` to create a locally trusted certificate.

1. **Install mkcert:**
```bash
brew install mkcert
brew install nss  # Only needed if you use Firefox

```


2. **Initialize Root CA:**
```bash
mkcert -install

```


3. **Generate Certs:**
Run this in your **Documents** folder to match the Java config below.
```bash
cd ~/Documents
mkcert localhost

```


*Output:* `localhost.pem` and `localhost-key.pem`

```
openssl req -new -key /Users/<username>/Documents/localhost-key.pem \
  -out /tmp/localhost.csr \
  -subj "/CN=localhost" \
  -config <(printf "[req]\ndistinguished_name=dn\nreq_extensions=ext\n[dn]\nCN=localhost\n[ext]\nsubjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1")

CAROOT=$(mkcert -CAROOT)

openssl x509 -req -in /tmp/localhost.csr \
  -CA "$CAROOT/rootCA.pem" \
  -CAkey "$CAROOT/rootCA-key.pem" \
  -CAcreateserial \
  -out /Users/<username>/Documents/localhost.pem \
  -days 10 -sha256 \
  -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1")
```

may need to add rootca in keychain and firefox authorities
---

## 2. Server Setup (Java)

Configure your Netty/Java server to use the generated certificates.

**Code Snippet:**

```java
QuicSslContext sslContext = QuicSslContextBuilder.forServer(
        new File("/Users/<username>/Documents/localhost-key.pem"), // Private Key
        null,
        new File("/Users/<username>/Documents/localhost.pem"))     // Public Cert
    .applicationProtocols(Http3.supportedApplicationProtocols())
    .build();

```

---

## 3. Client Setup (HTML)



**Use this in html to test webtrasnport all uni/bi/datagram apis**
***Run server***
```
 cd /
sudo http-server -S \
-C /Users/<username>/Documents/localhost.pem \
-K /Users/<username>/Documents/localhost-key.pem \
-p 8443
```
***Navigate to html***
```

https://localhost:8443/Users/<username>/Documents/GitHub/webtransport4j-incubator/native-wt-test.html
or
https://localhost:8443/Users/<username>/Documents/GitHub/webtransport4j-incubator/socketio-wt-test.html
```

**If you are using firefox**
add rootCA of mkcert in manage certificate -> authorities
&
about:config
```
network.http.http3.disable_when_third_party_roots_found	false		
network.http.http3.enable_localhost	true		
network.http.http3.enabled	true
```
