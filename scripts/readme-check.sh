set -euo pipefail

sudo() {
	"$@"
}

make loophole
ln -sf /app/bin/loophole-linux-$(go env GOARCH) /usr/local/bin/loophole
export PATH=/usr/local/bin:$PATH

TEST_ROOT=/tmp/readme-verify
rm -rf "$TEST_ROOT" /tmp/mnt-myclone /tmp/mnt-myclone2
mkdir -p "$TEST_ROOT"
cd "$TEST_ROOT"

STORE_URL="http://s3:9000/testbucket/readme-$(date +%s)"

loophole format "$STORE_URL"

loophole create "$STORE_URL" myvolume >/tmp/readme-myvolume.log 2>&1 &
OWNER_PID=$!
for i in $(seq 1 60); do
	if mountpoint -q myvolume 2>/dev/null; then
		break
	fi
	sleep 1
done
mountpoint -q myvolume

printf hello | sudo tee ./myvolume/greeting.txt >/dev/null
loophole checkpoint ./myvolume
CHECKPOINT_ID=$(loophole checkpoints ./myvolume | awk 'NR==1 {print $1}')
test -n "$CHECKPOINT_ID"

loophole clone ./myvolume myclone
loophole clone --from-checkpoint "$CHECKPOINT_ID" "$STORE_URL" myvolume myclone2

mkdir -p /tmp/mnt-myclone /tmp/mnt-myclone2

loophole mount "$STORE_URL" myclone /tmp/mnt-myclone >/tmp/readme-myclone.log 2>&1 &
CLONE1_PID=$!
for i in $(seq 1 60); do
	if mountpoint -q /tmp/mnt-myclone 2>/dev/null; then
		break
	fi
	sleep 1
done
mountpoint -q /tmp/mnt-myclone

loophole mount "$STORE_URL" myclone2 /tmp/mnt-myclone2 >/tmp/readme-myclone2.log 2>&1 &
CLONE2_PID=$!
for i in $(seq 1 60); do
	if mountpoint -q /tmp/mnt-myclone2 2>/dev/null; then
		break
	fi
	sleep 1
done
mountpoint -q /tmp/mnt-myclone2

test "$(cat /tmp/mnt-myclone/greeting.txt)" = "hello"
test "$(cat /tmp/mnt-myclone2/greeting.txt)" = "hello"

loophole shutdown ./myvolume
loophole shutdown /tmp/mnt-myclone
loophole shutdown /tmp/mnt-myclone2

wait "$OWNER_PID"
wait "$CLONE1_PID"
wait "$CLONE2_PID"

echo "README flow OK"
