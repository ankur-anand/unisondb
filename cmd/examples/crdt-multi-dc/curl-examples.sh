#!/bin/bash

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Configuration
DC1_URL="http://localhost:8001"
DC2_URL="http://localhost:8002"
RELAYER_URL="http://localhost:8003"
DC1_NAMESPACE="ad-campaign-dc1"
DC2_NAMESPACE="ad-campaign-dc2"

print_header() {
    echo ""
    echo -e "${BLUE}╔═════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║  $1${NC}"
    echo -e "${BLUE}╚═════════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_subheader() {
    echo ""
    echo -e "${YELLOW}▶ $1${NC}"
    echo ""
}

print_success() {
    echo -e "${GREEN} $1${NC}"
}

print_info() {
    echo -e "${BLUE} $1${NC}"
}

write_dc1() {
    local key=$1
    local value=$2
    local show_output=${3:-true}

    if [ "$show_output" = true ]; then
        echo -e "${GREEN}→ DC1:${NC} Writing key '${key}' to namespace '${DC1_NAMESPACE}'"
    fi

    curl -s -X PUT "${DC1_URL}/api/v1/${DC1_NAMESPACE}/kv/${key}" \
        -H "Content-Type: application/json" \
        -d "{\"value\": \"$(echo -n "$value" | base64)\"}"

    if [ "$show_output" = true ]; then
        echo ""
    fi
}

write_dc2() {
    local key=$1
    local value=$2
    local show_output=${3:-true}

    if [ "$show_output" = true ]; then
        echo -e "${GREEN}→ DC2:${NC} Writing key '${key}' to namespace '${DC2_NAMESPACE}'"
    fi

    curl -s -X PUT "${DC2_URL}/api/v1/${DC2_NAMESPACE}/kv/${key}" \
        -H "Content-Type: application/json" \
        -d "{\"value\": \"$(echo -n "$value" | base64)\"}"

    if [ "$show_output" = true ]; then
        echo ""
    fi
}

read_relayer() {
    local namespace=$1
    local key=$2

    echo -e "${BLUE}→ Relayer:${NC} Reading key '${key}' from namespace '${namespace}'"
    curl -s "${RELAYER_URL}/api/v1/${namespace}/kv/${key}" | jq
    echo ""
}

# Main script
clear
print_header "UnisonDB CRDT Client - Curl Request Examples"

print_info "This script demonstrates various CRDT use cases"
print_info "Make sure all services are running before proceeding:"
print_info "  - DC1 on port 8001"
print_info "  - DC2 on port 8002"
print_info "  - Relayer on port 8003"
print_info "  - Golang CRDT client (go run main.go)"
echo ""
read -p "Press Enter to continue..."

# ==============================================================================
# USE CASE 1: Basic LWW-Register (Last-Write-Wins)
# ==============================================================================
print_header "USE CASE 1: Basic LWW-Register (Last-Write-Wins)"

print_subheader "Scenario: User updates their status in two different regions"

print_info "Step 1: User sets status to 'online' in DC1"
TIMESTAMP1=$(date +%s)000
VALUE1=$(cat <<EOF
{
  "value": "online",
  "timestamp": $TIMESTAMP1,
  "replica": "$DC1_NAMESPACE"
}
EOF
)
write_dc1 "lww:user-status" "$VALUE1"
sleep 1

print_info "Step 2: User sets status to 'away' in DC2 (2 seconds later)"
TIMESTAMP2=$(($TIMESTAMP1 + 2000))
VALUE2=$(cat <<EOF
{
  "value": "away",
  "timestamp": $TIMESTAMP2,
  "replica": "$DC2_NAMESPACE"
}
EOF
)
write_dc2 "lww:user-status" "$VALUE2"
sleep 1

print_success "Result: 'away' wins (newer timestamp: $TIMESTAMP2 > $TIMESTAMP1)"
print_info "Check your Golang client output to see the CRDT state"
echo ""
read -p "Press Enter to continue to next use case..."

# ==============================================================================
# USE CASE 2: Concurrent Writes with Same Timestamp (Tie-breaker)
# ==============================================================================
print_header "USE CASE 2: Concurrent Writes - Replica ID Tie-breaker"

print_subheader "Scenario: Two regions update user profile simultaneously"

print_info "Both DCs write with IDENTICAL timestamp (simulating clock synchronization)"
TIMESTAMP3=$(date +%s)000
VALUE3=$(cat <<EOF
{
  "value": "Profile updated in DC1",
  "timestamp": $TIMESTAMP3,
  "replica": "$DC1_NAMESPACE"
}
EOF
)
VALUE4=$(cat <<EOF
{
  "value": "Profile updated in DC2",
  "timestamp": $TIMESTAMP3,
  "replica": "$DC2_NAMESPACE"
}
EOF
)

print_info "Writing to both DCs concurrently..."
write_dc1 "lww:user-profile" "$VALUE3" false &
write_dc2 "lww:user-profile" "$VALUE4" false &
wait
echo ""

print_success "Result: DC with lexicographically higher replica ID wins"
print_info "'$DC2_NAMESPACE' > '$DC1_NAMESPACE' → DC2 wins the tie-breaker"
echo ""
read -p "Press Enter to continue to next use case..."

# ==============================================================================
# USE CASE 3: G-Counter (Distributed Counter)
# ==============================================================================
print_header "USE CASE 3: G-Counter - Distributed Page View Counter"

print_subheader "Scenario: Two datacenters track page views independently"

print_info "DC1 serves 5 page views..."
for i in {1..5}; do
    COUNTER_VALUE=$(cat <<EOF
{
  "replica": "$DC1_NAMESPACE",
  "count": $i
}
EOF
)
    write_dc1 "counter:page-views" "$COUNTER_VALUE" false
    sleep 0.3
done
print_success "DC1: 5 page views"

print_info "DC2 serves 3 page views..."
for i in {1..3}; do
    COUNTER_VALUE=$(cat <<EOF
{
  "replica": "$DC2_NAMESPACE",
  "count": $i
}
EOF
)
    write_dc2 "counter:page-views" "$COUNTER_VALUE" false
    sleep 0.3
done
print_success "DC2: 3 page views"

echo ""
print_success "Result: Total = 8 (5 from DC1 + 3 from DC2)"
print_info "G-Counter automatically sums counts from all replicas"
echo ""
read -p "Press Enter to continue to next use case..."

# ==============================================================================
# USE CASE 4: Multiple Independent Counters
# ==============================================================================
print_header "USE CASE 4: Multiple Independent G-Counters"

print_subheader "Scenario: Track different metrics across datacenters"

print_info "Tracking API calls counter..."
for i in {1..10}; do
    COUNTER_VALUE=$(cat <<EOF
{
  "replica": "$DC1_NAMESPACE",
  "count": $i
}
EOF
)
    write_dc1 "counter:api-calls" "$COUNTER_VALUE" false
    sleep 0.2
done
print_success "DC1: 10 API calls"

for i in {1..7}; do
    COUNTER_VALUE=$(cat <<EOF
{
  "replica": "$DC2_NAMESPACE",
  "count": $i
}
EOF
)
    write_dc2 "counter:api-calls" "$COUNTER_VALUE" false
    sleep 0.2
done
print_success "DC2: 7 API calls"

print_info "Tracking DB queries counter..."
for i in {1..15}; do
    COUNTER_VALUE=$(cat <<EOF
{
  "replica": "$DC1_NAMESPACE",
  "count": $i
}
EOF
)
    write_dc1 "counter:db-queries" "$COUNTER_VALUE" false
    sleep 0.15
done
print_success "DC1: 15 DB queries"

echo ""
print_success "Result:"
print_success "  • API calls: 17 total (10 + 7)"
print_success "  • DB queries: 15 total (15 + 0)"
print_info "Each counter operates independently"
echo ""
read -p "Press Enter to continue to next use case..."

# ==============================================================================
# USE CASE 5: Out-of-Order Delivery (Stale Write Rejection)
# ==============================================================================
print_header "USE CASE 5: Out-of-Order Delivery - Stale Write Rejection"

print_subheader "Scenario: Network delay causes old write to arrive after new write"

print_info "Step 1: Write NEW value with current timestamp"
TIMESTAMP5=$(date +%s)000
VALUE5=$(cat <<EOF
{
  "value": "latest-config-v2",
  "timestamp": $TIMESTAMP5,
  "replica": "$DC2_NAMESPACE"
}
EOF
)
write_dc2 "lww:config" "$VALUE5"
sleep 1

print_info "Step 2: Write OLD value with past timestamp (simulating delayed message)"
TIMESTAMP6=$(($TIMESTAMP5 - 10000))  # 10 seconds in the past
VALUE6=$(cat <<EOF
{
  "value": "old-config-v1",
  "timestamp": $TIMESTAMP6,
  "replica": "$DC1_NAMESPACE"
}
EOF
)
write_dc1 "lww:config" "$VALUE6"
sleep 1

print_success "Result: Old value is REJECTED (stale timestamp)"
print_info "CRDT client will show: 'LWW-Register ignored (stale)'"
print_info "Final value: 'latest-config-v2' (timestamp $TIMESTAMP5 > $TIMESTAMP6)"
echo ""
read -p "Press Enter to continue to next use case..."

# ==============================================================================
# USE CASE 6: Feature Flag Management
# ==============================================================================
print_header "USE CASE 6: Feature Flag Management with LWW-Register"

print_subheader "Scenario: Enable/disable features across multiple datacenters"

print_info "Enabling feature 'dark-mode' in DC1"
TIMESTAMP7=$(date +%s)000
VALUE7=$(cat <<EOF
{
  "value": {"enabled": true, "rollout_percentage": 50},
  "timestamp": $TIMESTAMP7,
  "replica": "$DC1_NAMESPACE"
}
EOF
)
write_dc1 "lww:feature:dark-mode" "$VALUE7"
sleep 1

print_info "Updating rollout to 100% in DC2"
TIMESTAMP8=$(($TIMESTAMP7 + 1000))
VALUE8=$(cat <<EOF
{
  "value": {"enabled": true, "rollout_percentage": 100},
  "timestamp": $TIMESTAMP8,
  "replica": "$DC2_NAMESPACE"
}
EOF
)
write_dc2 "lww:feature:dark-mode" "$VALUE8"
sleep 1

print_success "Result: Feature flag updated to 100% rollout"
print_info "All clients see the latest configuration instantly"
echo ""
read -p "Press Enter to continue to next use case..."

# ==============================================================================
# USE CASE 7: User Presence System
# ==============================================================================
print_header "USE CASE 7: User Presence System (Online/Away/Offline)"

print_subheader "Scenario: Track user presence across multiple devices"

print_info "User logs in from laptop (DC1)"
TIMESTAMP9=$(date +%s)000
VALUE9=$(cat <<EOF
{
  "value": {"status": "online", "device": "laptop", "location": "us-east"},
  "timestamp": $TIMESTAMP9,
  "replica": "$DC1_NAMESPACE"
}
EOF
)
write_dc1 "lww:presence:user123" "$VALUE9"
sleep 1

print_info "User switches to mobile (DC2) 2 seconds later"
TIMESTAMP10=$(($TIMESTAMP9 + 2000))
VALUE10=$(cat <<EOF
{
  "value": {"status": "online", "device": "mobile", "location": "eu-west"},
  "timestamp": $TIMESTAMP10,
  "replica": "$DC2_NAMESPACE"
}
EOF
)
write_dc2 "lww:presence:user123" "$VALUE10"
sleep 1

print_info "User goes away (DC2)"
TIMESTAMP11=$(($TIMESTAMP10 + 3000))
VALUE11=$(cat <<EOF
{
  "value": {"status": "away", "device": "mobile", "location": "eu-west"},
  "timestamp": $TIMESTAMP11,
  "replica": "$DC2_NAMESPACE"
}
EOF
)
write_dc2 "lww:presence:user123" "$VALUE11"
sleep 1

print_success "Result: Final status is 'away' (most recent update)"
print_info "All clients see the converged presence state"
echo ""
read -p "Press Enter to continue to next use case..."

# ==============================================================================
# USE CASE 8: Shopping Cart Synchronization
# ==============================================================================
print_header "USE CASE 8: Shopping Cart Synchronization Across Devices"

print_subheader "Scenario: User adds items to cart on different devices"

print_info "User adds item on mobile (DC1)"
TIMESTAMP12=$(date +%s)000
VALUE12=$(cat <<EOF
{
  "value": {"items": ["laptop", "mouse"], "total": 1299.99},
  "timestamp": $TIMESTAMP12,
  "replica": "$DC1_NAMESPACE"
}
EOF
)
write_dc1 "lww:cart:user456" "$VALUE12"
sleep 1

print_info "User adds another item on desktop (DC2)"
TIMESTAMP13=$(($TIMESTAMP12 + 1000))
VALUE13=$(cat <<EOF
{
  "value": {"items": ["laptop", "mouse", "keyboard"], "total": 1399.99},
  "timestamp": $TIMESTAMP13,
  "replica": "$DC2_NAMESPACE"
}
EOF
)
write_dc2 "lww:cart:user456" "$VALUE13"
sleep 1

print_success "Result: Cart updated to include all 3 items"
print_info "Seamless cart sync across devices"
echo ""
read -p "Press Enter to continue to next use case..."

# ==============================================================================
# USE CASE 9: Distributed Rate Limiting
# ==============================================================================
print_header "USE CASE 9: Distributed Rate Limiting with G-Counter"

print_subheader "Scenario: Track API requests per user across multiple regions"

print_info "User makes 30 requests in DC1..."
for i in {1..30}; do
    COUNTER_VALUE=$(cat <<EOF
{
  "replica": "$DC1_NAMESPACE",
  "count": $i
}
EOF
)
    write_dc1 "counter:ratelimit:user789" "$COUNTER_VALUE" false
    sleep 0.1
done
print_success "DC1: 30 requests"

print_info "User makes 25 requests in DC2..."
for i in {1..25}; do
    COUNTER_VALUE=$(cat <<EOF
{
  "replica": "$DC2_NAMESPACE",
  "count": $i
}
EOF
)
    write_dc2 "counter:ratelimit:user789" "$COUNTER_VALUE" false
    sleep 0.1
done
print_success "DC2: 25 requests"

echo ""
print_success "Result: Total = 55 requests"
print_info "If limit is 100 requests, user can make 45 more"
echo ""
read -p "Press Enter to continue to next use case..."

# ==============================================================================
# USE CASE 10: Live Dashboard Metrics
# ==============================================================================
print_header "USE CASE 10: Live Dashboard - Real-Time Metrics Aggregation"

print_subheader "Scenario: Aggregate metrics from multiple datacenters in real-time"

print_info "Tracking active sessions..."
for i in {1..20}; do
    COUNTER_VALUE=$(cat <<EOF
{
  "replica": "$DC1_NAMESPACE",
  "count": $i
}
EOF
)
    write_dc1 "counter:dashboard:active-sessions" "$COUNTER_VALUE" false
    sleep 0.15
done
print_success "DC1: 20 active sessions"

for i in {1..15}; do
    COUNTER_VALUE=$(cat <<EOF
{
  "replica": "$DC2_NAMESPACE",
  "count": $i
}
EOF
)
    write_dc2 "counter:dashboard:active-sessions" "$COUNTER_VALUE" false
    sleep 0.15
done
print_success "DC2: 15 active sessions"

print_info "Tracking error counts..."
for i in {1..3}; do
    COUNTER_VALUE=$(cat <<EOF
{
  "replica": "$DC1_NAMESPACE",
  "count": $i
}
EOF
)
    write_dc1 "counter:dashboard:errors" "$COUNTER_VALUE" false
    sleep 0.2
done
print_success "DC1: 3 errors"

for i in {1..1}; do
    COUNTER_VALUE=$(cat <<EOF
{
  "replica": "$DC2_NAMESPACE",
  "count": $i
}
EOF
)
    write_dc2 "counter:dashboard:errors" "$COUNTER_VALUE" false
    sleep 0.2
done
print_success "DC2: 1 error"

echo ""
print_success "Dashboard shows:"
print_success "  • Active sessions: 35 (20 + 15)"
print_success "  • Total errors: 4 (3 + 1)"
echo ""
read -r -p "Done Enter to close..."

echo ""
