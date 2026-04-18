#!/bin/bash

BASE_URL="http://localhost:8080/api/v1/acp"
RABBIT_URL="http://localhost:15672/api"
RABBIT_AUTH="guest:guest"
REDIS_CONTAINER="acpcw2template-redis-1"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0

pass() { echo -e "${GREEN}  ✓ PASS${NC} — $1"; PASS=$((PASS + 1)); }
fail() { echo -e "${RED}  ✗ FAIL${NC} — $1"; FAIL=$((FAIL + 1)); }
section() { echo -e "\n${YELLOW}══════════════════════════════════════${NC}"; echo -e "${YELLOW}  $1${NC}"; echo -e "${YELLOW}══════════════════════════════════════${NC}"; }

redis_get() { docker exec $REDIS_CONTAINER redis-cli get "$1" 2>/dev/null | tr -d '\r'; }
redis_flush() { docker exec $REDIS_CONTAINER redis-cli flushall > /dev/null; }

purge_queue() {
    curl -s -u $RABBIT_AUTH -X DELETE "$RABBIT_URL/queues/%2F/$1/contents" > /dev/null
}

create_queue() {
    curl -s -u $RABBIT_AUTH -X PUT "$RABBIT_URL/queues/%2F/$1" \
        -H "Content-Type: application/json" -d '{"durable":false}' > /dev/null
}

publish_normal() {
    local queue=$1 key=$2 version=$3 value=$4
    curl -s -u $RABBIT_AUTH -X POST "$RABBIT_URL/exchanges/%2F//publish" \
        -H "Content-Type: application/json" \
        -d "{\"properties\":{},\"routing_key\":\"$queue\",\"payload\":\"{\\\"key\\\":\\\"$key\\\",\\\"version\\\":$version,\\\"value\\\":$value}\",\"payload_encoding\":\"string\"}" > /dev/null
}

publish_tombstone() {
    curl -s -u $RABBIT_AUTH -X POST "$RABBIT_URL/exchanges/%2F//publish" \
        -H "Content-Type: application/json" \
        -d "{\"properties\":{},\"routing_key\":\"$1\",\"payload\":\"{\\\"key\\\":\\\"TOMBSTONE\\\"}\",\"payload_encoding\":\"string\"}" > /dev/null
}

call_transform() {
    curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE_URL/transformMessages" \
        -H "Content-Type: application/json" \
        -d "{\"readQueue\":\"inQueue\",\"writeQueue\":\"outQueue\",\"messageCount\":$1}"
}

read_output() {
    curl -s "$BASE_URL/messages/rabbitmq/outQueue/500"
}

# Check if any message in the output array has field=value
has_field_value() {
    local output=$1 field=$2 value=$3
    echo "$output" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for item in data:
    try:
        obj = json.loads(item)
        if str(obj.get('$field', '')) == '$value':
            print('found')
            exit(0)
    except:
        pass
" 2>/dev/null | grep -q "found"
}

# Check if any message contains a top-level key
has_key() {
    local output=$1 key=$2
    echo "$output" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for item in data:
    try:
        obj = json.loads(item)
        if '$key' in obj:
            print('found')
            exit(0)
    except:
        pass
" 2>/dev/null | grep -q "found"
}

# Get specific field from Nth message (0-indexed)
get_msg_field() {
    local output=$1 idx=$2 field=$3
    echo "$output" | python3 -c "
import json, sys
data = json.load(sys.stdin)
if len(data) > $idx:
    obj = json.loads(data[$idx])
    print(obj.get('$field', ''))
" 2>/dev/null
}

# SETUP
section "SETUP"
create_queue "inQueue"
create_queue "outQueue"
echo "Queues created"

# ============================================================
section "SCENARIO 1 — New key gets +10.5"

redis_flush
purge_queue "inQueue"; purge_queue "outQueue"
publish_normal "inQueue" "ABC" 1 100.0

RESP=$(call_transform 1)
[ "$RESP" = "200" ] && pass "Returns 200" || fail "Expected 200, got $RESP"

OUT=$(read_output)
has_field_value "$OUT" "value" "110.5" && pass "New key: value=110.5" || fail "value wrong: $OUT"
has_field_value "$OUT" "key" "ABC" && pass "key field preserved" || fail "key missing: $OUT"
has_field_value "$OUT" "version" "1" && pass "version field preserved" || fail "version missing: $OUT"

VER=$(redis_get "ABC")
[ "$VER" = "1" ] && pass "Redis stores ABC=1" || fail "Redis version wrong: $VER"

# ============================================================
section "SCENARIO 2 — Same version passes through"

redis_flush
purge_queue "inQueue"; purge_queue "outQueue"
publish_normal "inQueue" "ABC" 1 100.0
publish_normal "inQueue" "ABC" 1 200.0

call_transform 2 > /dev/null

OUT=$(read_output)
VAL1=$(get_msg_field "$OUT" 0 "value")
VAL2=$(get_msg_field "$OUT" 1 "value")
[ "$VAL1" = "110.5" ] && pass "First message: 110.5" || fail "First wrong: $VAL1"
[ "$VAL2" = "200.0" ] && pass "Same version: passed through 200.0" || fail "Second wrong: $VAL2"

# ============================================================
section "SCENARIO 3 — Newer version gets +10.5"

redis_flush
purge_queue "inQueue"; purge_queue "outQueue"
publish_normal "inQueue" "ABC" 1 100.0
publish_normal "inQueue" "ABC" 3 400.0

call_transform 2 > /dev/null

OUT=$(read_output)
VAL2=$(get_msg_field "$OUT" 1 "value")
[ "$VAL2" = "410.5" ] && pass "Newer version: 410.5" || fail "Newer version wrong: $VAL2"
VER=$(redis_get "ABC")
[ "$VER" = "3" ] && pass "Redis updated to version 3" || fail "Redis version wrong: $VER"

# ============================================================
section "SCENARIO 4 — Older version passes through"

redis_flush
purge_queue "inQueue"; purge_queue "outQueue"
publish_normal "inQueue" "ABC" 3 400.0
publish_normal "inQueue" "ABC" 1 100.0

call_transform 2 > /dev/null

OUT=$(read_output)
VAL2=$(get_msg_field "$OUT" 1 "value")
[ "$VAL2" = "100.0" ] && pass "Older version: passed through 100.0" || fail "Older wrong: $VAL2"
VER=$(redis_get "ABC")
[ "$VER" = "3" ] && pass "Redis still holds version 3" || fail "Redis version wrong: $VER"

# ============================================================
section "SCENARIO 5 — Tombstone writes summary and clears keys"

redis_flush
purge_queue "inQueue"; purge_queue "outQueue"
publish_normal "inQueue" "ABC" 1 100.0
publish_normal "inQueue" "ABC" 1 200.0
publish_tombstone "inQueue"

call_transform 3 > /dev/null

OUT=$(read_output)
has_key "$OUT" "totalMessagesWritten" && pass "Tombstone summary written" || fail "Summary missing"

TVW=$(get_msg_field "$OUT" 2 "totalValueWritten")
[ "$TVW" = "310.5" ] && pass "totalValueWritten=310.5" || fail "totalValueWritten wrong: $TVW"

TA=$(get_msg_field "$OUT" 2 "totalAdded")
[ "$TA" = "10.5" ] && pass "totalAdded=10.5" || fail "totalAdded wrong: $TA"

TW=$(get_msg_field "$OUT" 2 "totalMessagesWritten")
[ "$TW" = "2" ] && pass "totalMessagesWritten=2" || fail "totalMessagesWritten wrong: $TW"

TP=$(get_msg_field "$OUT" 2 "totalMessagesProcessed")
[ "$TP" = "3" ] && pass "totalMessagesProcessed=3" || fail "totalMessagesProcessed wrong: $TP"

VER=$(redis_get "ABC")
[ -z "$VER" ] && pass "Redis key ABC cleared" || fail "Redis key not cleared: $VER"

# ============================================================
section "SCENARIO 6 — After tombstone key treated as new"

redis_flush
purge_queue "inQueue"; purge_queue "outQueue"
publish_normal "inQueue" "ABC" 1 100.0
publish_tombstone "inQueue"
publish_normal "inQueue" "ABC" 2 200.0

call_transform 3 > /dev/null

OUT=$(read_output)
VAL3=$(get_msg_field "$OUT" 2 "value")
[ "$VAL3" = "210.5" ] && pass "After tombstone: ABC v2 → 210.5" || fail "After tombstone wrong: $VAL3"

# ============================================================
section "SCENARIO 7 — Multiple tombstones, counters accumulate"

redis_flush
purge_queue "inQueue"; purge_queue "outQueue"
publish_normal "inQueue" "ABC" 1 100.0
publish_tombstone "inQueue"
publish_normal "inQueue" "DEF" 1 50.0
publish_tombstone "inQueue"

call_transform 4 > /dev/null

OUT=$(read_output)
TP=$(get_msg_field "$OUT" 3 "totalMessagesProcessed")
[ "$TP" = "4" ] && pass "Second tombstone: totalMessagesProcessed=4" || fail "totalMessagesProcessed wrong: $TP"

TRU=$(get_msg_field "$OUT" 3 "totalRedisUpdates")
[ "$TRU" = "2" ] && pass "Second tombstone: totalRedisUpdates=2" || fail "totalRedisUpdates wrong: $TRU"

TW=$(get_msg_field "$OUT" 3 "totalMessagesWritten")
[ "$TW" = "3" ] && pass "Second tombstone: totalMessagesWritten=3" || fail "totalMessagesWritten wrong: $TW"

# ============================================================
section "SCENARIO 8 — Multi-call persistence (1 message at a time)"

redis_flush
purge_queue "inQueue"; purge_queue "outQueue"

publish_normal "inQueue" "ABC" 1 100.0
call_transform 1 > /dev/null
VER=$(redis_get "ABC")
[ "$VER" = "1" ] && pass "After call 1: Redis has ABC=1" || fail "Redis wrong: $VER"

publish_normal "inQueue" "ABC" 2 200.0
call_transform 1 > /dev/null
VER=$(redis_get "ABC")
[ "$VER" = "2" ] && pass "After call 2: Redis updated to ABC=2" || fail "Redis wrong: $VER"

publish_tombstone "inQueue"
call_transform 1 > /dev/null
VER=$(redis_get "ABC")
[ -z "$VER" ] && pass "After tombstone: ABC cleared" || fail "ABC not cleared: $VER"

OUT=$(read_output)
TW=$(get_msg_field "$OUT" 2 "totalMessagesWritten")
TP=$(get_msg_field "$OUT" 2 "totalMessagesProcessed")
[ "$TW" = "2" ] && pass "Multi-call: totalMessagesWritten=2" || fail "totalMessagesWritten wrong: $TW"
[ "$TP" = "3" ] && pass "Multi-call: totalMessagesProcessed=3" || fail "totalMessagesProcessed wrong: $TP"

# ============================================================
section "SCENARIO 9 — Full spec example"

redis_flush
purge_queue "inQueue"; purge_queue "outQueue"
publish_normal "inQueue" "ABC" 1 100.0
publish_normal "inQueue" "ABC" 1 200.0
publish_normal "inQueue" "ABC" 3 400.0
publish_normal "inQueue" "ABC" 2 200.0
publish_tombstone "inQueue"
publish_normal "inQueue" "ABC" 2 200.0

call_transform 6 > /dev/null

OUT=$(read_output)
VAL1=$(get_msg_field "$OUT" 0 "value")
VAL2=$(get_msg_field "$OUT" 1 "value")
VAL3=$(get_msg_field "$OUT" 2 "value")
VAL4=$(get_msg_field "$OUT" 3 "value")
VAL6=$(get_msg_field "$OUT" 5 "value")

[ "$VAL1" = "110.5" ] && pass "Spec msg1: ABC v1 → 110.5" || fail "msg1 wrong: $VAL1"
[ "$VAL2" = "200.0" ] && pass "Spec msg2: ABC v1 same → 200.0" || fail "msg2 wrong: $VAL2"
[ "$VAL3" = "410.5" ] && pass "Spec msg3: ABC v3 newer → 410.5" || fail "msg3 wrong: $VAL3"
[ "$VAL4" = "200.0" ] && pass "Spec msg4: ABC v2 older → 200.0" || fail "msg4 wrong: $VAL4"
has_key "$OUT" "totalMessagesWritten" && pass "Spec tombstone summary written" || fail "Tombstone missing"
[ "$VAL6" = "210.5" ] && pass "Spec msg6: after tombstone → 210.5" || fail "msg6 wrong: $VAL6"

# ============================================================
section "SUMMARY"
echo -e "  ${GREEN}Passed: $PASS${NC}"
echo -e "  ${RED}Failed: $FAIL${NC}"