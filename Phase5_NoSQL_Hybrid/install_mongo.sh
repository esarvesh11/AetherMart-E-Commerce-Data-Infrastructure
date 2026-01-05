# From your home directory on EC2
mkdir -p ~/aethermart && cd ~/aethermart

# Save the installer
cat > install_mongodb.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
MONGO_VERSION="7.0"
sudo apt-get update -y
sudo apt-get install -y gnupg curl
curl -fsSL https://pgp.mongodb.com/server-${MONGO_VERSION}.asc | sudo gpg --dearmor -o /usr/share/keyrings/mongodb-server-${MONGO_VERSION}.gpg
echo "deb [signed-by=/usr/share/keyrings/mongodb-server-${MONGO_VERSION}.gpg] http://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/${MONGO_VERSION} multiverse" | \
  sudo tee /etc/apt/sources.list.d/mongodb-org-${MONGO_VERSION}.list > /dev/null
sudo apt-get update -y && sudo apt-get install -y mongodb-org
sudo systemctl enable --now mongod

# Create app user with readWrite on 'aethermart'
MONGO_APP_PASS=${MONGO_APP_PASS:-$(openssl rand -base64 24)}
export MONGO_APP_PASS
cat <<'MJS' >/tmp/init.js
const user = "aether_app";
const pass = process.env.MONGO_APP_PASS;
const dbname = "aethermart";
db = db.getSiblingDB(dbname);
try {
  db.createUser({ user, pwd: pass, roles: [{ role: "readWrite", db: dbname }]});
  print("User created");
} catch(e) { print(e); }
MJS
MONGO_APP_PASS="$MONGO_APP_PASS" mongosh --quiet /tmp/init.js || true
rm -f /tmp/init.js
echo "App user password: $MONGO_APP_PASS"
EOF

chmod +x install_mongodb.sh
./install_mongodb.sh