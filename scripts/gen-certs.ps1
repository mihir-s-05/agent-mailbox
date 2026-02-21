$ErrorActionPreference = "Stop"

New-Item -ItemType Directory -Force -Path certs | Out-Null

openssl genrsa -out certs/ca.key 4096
openssl req -x509 -new -nodes -key certs/ca.key -sha256 -days 3650 -subj "/CN=Mailbox Local CA" -out certs/ca.crt

openssl genrsa -out certs/server.key 2048
openssl req -new -key certs/server.key -subj "/CN=127.0.0.1" -out certs/server.csr
@"
subjectAltName = IP:127.0.0.1,DNS:localhost
extendedKeyUsage = serverAuth
"@ | Set-Content certs/server-ext.cnf
openssl x509 -req -in certs/server.csr -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial -out certs/server.crt -days 365 -sha256 -extfile certs/server-ext.cnf

openssl genrsa -out certs/client.key 2048
openssl req -new -key certs/client.key -subj "/CN=mailbox-client" -out certs/client.csr
@"
extendedKeyUsage = clientAuth
"@ | Set-Content certs/client-ext.cnf
openssl x509 -req -in certs/client.csr -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial -out certs/client.crt -days 365 -sha256 -extfile certs/client-ext.cnf
openssl pkcs12 -export -inkey certs/client.key -in certs/client.crt -out certs/client.pfx -passout pass:

Write-Host "Generated certs in ./certs"
