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
redis_hget() { docker exec $REDIS_CONTAINER redis-cli hget "$1" "$2" 2>/dev/null | tr -d '\r'; }
redis_flush() { docker exec $REDIS_CONTAINER redis-cli flushall > /dev/null; }

purge_queue() {
    curl -s -u $RABBIT_AUTH -X DELETE "$RABBIT_URL/queues/%2F/$1/contents" > /dev/null
}

call_splitter() {
    curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE_URL/splitter" \
        -H "Content-Type: application/json" \
        -d "{\"readQueue\":\"splitQueue\",\"writeTopicOdd\":\"oddTopic\",\"redisHashOdd\":\"hashOdd\",\"writeTopicEven\":\"evenTopic\",\"redisHashEven\":\"hashEven\",\"messageCount\":$1}"
}

publish_msg() {
    local queue=$1
    local id=$2
    local value=$3
    local data=$4
    curl -s -u $RABBIT_AUTH -X POST "$RABBIT_URL/exchanges/%2F//publish" \
        -H "Content-Type: application/json" \
        -d "{\"properties\":{},\"routing_key\":\"$queue\",\"payload\":\"{\\\"Id\\\":$id,\\\"Value\\\":$value,\\\"AdditionalData\\\":\\\"$data\\\"}\",\"payload_encoding\":\"string\"}" > /dev/null
}

# SETUP
section "SETUP"
curl -s -u $RABBIT_AUTH -X PUT "$RABBIT_URL/queues/%2F/splitQueue" \
    -H "Content-Type: application/json" -d '{"durable":true}' > /dev/null
echo "Queues created"

# ============================================================
section "SCENARIO 1 — Basic even/odd split, fresh Redis"

redis_flush
purge_queue "splitQueue"
publish_msg "splitQueue" 1 0.5 "one"
publish_msg "splitQueue" 2 1.5 "two"
publish_msg "splitQueue" 3 2.0 "three"
publish_msg "splitQueue" 4 3.0 "four"

RESP=$(call_splitter 4)
[ "$RESP" = "200" ] && pass "Returns 200" || fail "Expected 200, got $RESP"

CE=$(redis_get "count_even"); CO=$(redis_get "count_odd")
[ "$CE" = "2" ] && pass "count_even=2" || fail "count_even wrong: $CE"
[ "$CO" = "2" ] && pass "count_odd=2" || fail "count_odd wrong: $CO"

AE=$(redis_get "average_even"); AO=$(redis_get "average_odd")
[ "$AE" = "2.25" ] && pass "average_even=2.25" || fail "average_even wrong: $AE"
[ "$AO" = "1.25" ] && pass "average_odd=1.25" || fail "average_odd wrong: $AO"

HASH2=$(redis_hget "hashEven" "2")
HASH4=$(redis_hget "hashEven" "4")
HASH1=$(redis_hget "hashOdd" "1")
HASH3=$(redis_hget "hashOdd" "3")
[ -n "$HASH2" ] && pass "hashEven has Id=2" || fail "hashEven missing Id=2"
[ -n "$HASH4" ] && pass "hashEven has Id=4" || fail "hashEven missing Id=4"
[ -n "$HASH1" ] && pass "hashOdd has Id=1" || fail "hashOdd missing Id=1"
[ -n "$HASH3" ] && pass "hashOdd has Id=3" || fail "hashOdd missing Id=3"

# ============================================================
section "SCENARIO 2 — Multiple calls accumulate"

purge_queue "splitQueue"
publish_msg "splitQueue" 1 0.5 "one"
publish_msg "splitQueue" 2 1.5 "two"
publish_msg "splitQueue" 3 2.0 "three"
publish_msg "splitQueue" 4 3.0 "four"

call_splitter 4 > /dev/null

CE=$(redis_get "count_even"); CO=$(redis_get "count_odd")
[ "$CE" = "4" ] && pass "Second call: count_even=4" || fail "count_even wrong: $CE"
[ "$CO" = "4" ] && pass "Second call: count_odd=4" || fail "count_odd wrong: $CO"
AE=$(redis_get "average_even")
[ "$AE" = "2.25" ] && pass "Average unchanged after same data: average_even=2.25" || fail "average_even wrong: $AE"

# ============================================================
section "SCENARIO 3 — All odd messages"

redis_flush
purge_queue "splitQueue"
publish_msg "splitQueue" 1 10.0 "a"
publish_msg "splitQueue" 3 20.0 "b"
publish_msg "splitQueue" 5 30.0 "c"

call_splitter 3 > /dev/null

CO=$(redis_get "count_odd"); CE=$(redis_get "count_even")
[ "$CO" = "3" ] && pass "All-odd: count_odd=3" || fail "count_odd wrong: $CO"
[ "$CE" = "0" ] && pass "All-odd: count_even=0" || fail "count_even wrong: $CE"
AO=$(redis_get "average_odd")
[ "$AO" = "20.0" ] && pass "All-odd: average_odd=20.0" || fail "average_odd wrong: $AO"

# ============================================================
section "SCENARIO 4 — All even messages"

redis_flush
purge_queue "splitQueue"
publish_msg "splitQueue" 2 10.0 "a"
publish_msg "splitQueue" 4 20.0 "b"
publish_msg "splitQueue" 6 30.0 "c"

call_splitter 3 > /dev/null

CE=$(redis_get "count_even"); CO=$(redis_get "count_odd")
[ "$CE" = "3" ] && pass "All-even: count_even=3" || fail "count_even wrong: $CE"
[ "$CO" = "0" ] && pass "All-even: count_odd=0" || fail "count_odd wrong: $CO"
AE=$(redis_get "average_even")
[ "$AE" = "20.0" ] && pass "All-even: average_even=20.0" || fail "average_even wrong: $AE"

# ============================================================
section "SCENARIO 5 — Duplicate Ids"

redis_flush
purge_queue "splitQueue"
publish_msg "splitQueue" 2 10.0 "first"
publish_msg "splitQueue" 2 90.0 "second"

call_splitter 2 > /dev/null

CE=$(redis_get "count_even")
[ "$CE" = "2" ] && pass "Duplicate Id: count_even=2" || fail "count_even wrong: $CE"
AE=$(redis_get "average_even")
[ "$AE" = "50.0" ] && pass "Duplicate Id: average_even=50.0" || fail "average_even wrong: $AE"
HASH=$(redis_hget "hashEven" "2")
echo "$HASH" | grep -q "90.0" && pass "Hash overwritten with latest (90.0)" || fail "Hash not overwritten: $HASH"

# ============================================================
section "SCENARIO 6 — Large batch (20 messages)"

redis_flush
purge_queue "splitQueue"
for i in $(seq 1 20); do
    publish_msg "splitQueue" $i $((i*10)).0 "msg$i"
done

call_splitter 20 > /dev/null

CE=$(redis_get "count_even"); CO=$(redis_get "count_odd")
[ "$CE" = "10" ] && pass "Large batch: count_even=10" || fail "count_even wrong: $CE"
[ "$CO" = "10" ] && pass "Large batch: count_odd=10" || fail "count_odd wrong: $CO"
AE=$(redis_get "average_even")
[ "$AE" = "110.0" ] && pass "Large batch: average_even=110.0" || fail "average_even wrong: $AE"

# ============================================================
section "SUMMARY"
echo -e "  ${GREEN}Passed: $PASS${NC}"
echo -e "  ${RED}Failed: $FAIL${NC}"