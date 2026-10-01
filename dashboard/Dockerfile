# Multi-stage production container for WebTransport4J Enterprise Dashboard
FROM nginx:1.27-alpine

# Set working directory and remove default nginx page
WORKDIR /usr/share/nginx/html
RUN rm -rf ./*

# Copy dashboard static assets
COPY index.html styles.css app.js README.md ./

# Custom hardened Nginx configuration with gzip, security headers, and caching
RUN cat <<'EOF' > /etc/nginx/conf.d/default.conf
server {
    listen 8080;
    server_name _;

    root /usr/share/nginx/html;
    index index.html;

    # Security headers
    add_header X-Frame-Options "DENY" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-XSS-Protection "1; mode=block" always;
    add_header Referrer-Policy "strict-origin-when-cross-origin" always;

    # Gzip compression
    gzip on;
    gzip_vary on;
    gzip_min_length 1024;
    gzip_proxied any;
    gzip_types text/plain text/css application/json application/javascript text/xml application/xml;

    location / {
        try_files $uri $uri/ /index.html;
    }

    # Health check for Kubernetes liveness/readiness probes
    location /healthz {
        access_log off;
        return 200 '{"status":"UP"}';
        add_header Content-Type application/json;
    }

    # Cache static assets
    location ~* \.(css|js|jpg|jpeg|png|gif|ico|svg|woff2)$ {
        expires 7d;
        add_header Cache-Control "public, no-transform";
    }
}
EOF

# Use non-root user for security compliance
USER nginx

# Container metadata & health check
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=3s --retries=3 \
  CMD wget --quiet --tries=1 --spider http://localhost:8080/healthz || exit 1

CMD ["nginx", "-g", "daemon off;"]
