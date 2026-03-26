#!/bin/bash
# ============================================================
# Preventive Health Research Pipeline - Service Manager
# ============================================================
# Usage:
#   ./run_services.sh                  # Start all core services
#   ./run_services.sh --scaled         # Start all + extra extraction worker
#   ./run_services.sh --services crawler,llm   # Start specific services
#   ./run_services.sh --stop           # Stop all services
#   ./run_services.sh --restart        # Restart all services
#   ./run_services.sh --status         # Show service status & health
#   ./run_services.sh --logs [service] # Tail logs (optional: specific service)
#   ./run_services.sh --build          # Rebuild and start all services
#   ./run_services.sh --trigger-pipeline  # Trigger a manual pipeline run
#   ./run_services.sh --trigger-notify    # Trigger notification-only run
#   ./run_services.sh --trigger-weekly-report  # Trigger weekly PDF report generation
#   ./run_services.sh --trigger-test-notification  # Send dummy Slack test articles and verify DB storage
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Service definitions
CORE_SERVICES="crawler dedup extraction-worker-1 llm notification orchestrator api-gateway"
ALL_SERVICES="$CORE_SERVICES extraction-worker-2"

# Health check endpoints (host_port -> service_name)
declare -A SERVICE_PORTS=(
    ["crawler"]=8011
    ["dedup"]=8002
    ["extraction-worker-1"]=8003
    ["llm"]=8004
    ["notification"]=8005
    ["orchestrator"]=8006
    ["api-gateway"]=8010
)

# ============================================================
# Helper Functions
# ============================================================

log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; }
log_header()  { echo -e "\n${CYAN}═══════════════════════════════════════${NC}"; echo -e "${CYAN} $1${NC}"; echo -e "${CYAN}═══════════════════════════════════════${NC}"; }

check_prerequisites() {
    if ! command -v docker &>/dev/null; then
        log_error "Docker is not installed"
        exit 1
    fi
    if ! command -v docker-compose &>/dev/null && ! docker compose version &>/dev/null 2>&1; then
        log_error "Docker Compose is not installed"
        exit 1
    fi
    if [ ! -f "docker-compose.yml" ]; then
        log_error "docker-compose.yml not found in $SCRIPT_DIR"
        exit 1
    fi
}

# Detect docker compose command (v1 vs v2)
get_compose_cmd() {
    if docker compose version &>/dev/null 2>&1; then
        echo "docker compose"
    else
        echo "docker-compose"
    fi
}

# ============================================================
# Commands
# ============================================================

cmd_start() {
    local services="$1"
    local build_flag="$2"
    local profile_flag="$3"

    log_header "Starting Services"
    local compose_cmd
    compose_cmd=$(get_compose_cmd)

    local cmd="$compose_cmd"
    if [ "$profile_flag" = "scaled" ]; then
        cmd="$cmd --profile scaled"
        log_info "Including scaled profile (extra extraction worker)"
    fi

    cmd="$cmd up -d"
    if [ "$build_flag" = "build" ]; then
        cmd="$cmd --build"
        log_info "Rebuilding images..."
    fi

    if [ -n "$services" ]; then
        log_info "Starting selected services: $services"
        cmd="$cmd $services"
    else
        log_info "Starting all core services..."
    fi

    eval "$cmd"

    log_success "Services started"
    echo ""
    cmd_health
}

cmd_stop() {
    log_header "Stopping Services"
    local compose_cmd
    compose_cmd=$(get_compose_cmd)
    $compose_cmd --profile scaled down
    log_success "All services stopped"
}

cmd_restart() {
    local services="$1"
    local profile_flag="$2"

    log_header "Restarting Services"
    local compose_cmd
    compose_cmd=$(get_compose_cmd)

    if [ -n "$services" ]; then
        log_info "Restarting: $services"
        $compose_cmd restart $services
    else
        local cmd="$compose_cmd"
        if [ "$profile_flag" = "scaled" ]; then
            cmd="$cmd --profile scaled"
        fi
        $cmd restart
    fi
    log_success "Services restarted"
    echo ""
    cmd_health
}

cmd_status() {
    log_header "Service Status"
    local compose_cmd
    compose_cmd=$(get_compose_cmd)
    $compose_cmd --profile scaled ps
}

cmd_health() {
    log_header "Service Health Checks"

    local all_healthy=true

    for service in $CORE_SERVICES; do
        local port="${SERVICE_PORTS[$service]}"
        local url="http://localhost:${port}/health"

        if response=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 3 --max-time 5 "$url" 2>/dev/null); then
            if [ "$response" = "200" ]; then
                log_success "$service (port $port) - healthy"
            else
                log_warn "$service (port $port) - HTTP $response"
                all_healthy=false
            fi
        else
            log_error "$service (port $port) - unreachable"
            all_healthy=false
        fi
    done

    echo ""
    if [ "$all_healthy" = true ]; then
        log_success "All services are healthy!"
    else
        log_warn "Some services are not healthy. Check logs with: ./run_services.sh --logs"
    fi
}

cmd_logs() {
    local service="$1"
    local compose_cmd
    compose_cmd=$(get_compose_cmd)

    if [ -n "$service" ]; then
        log_info "Tailing logs for: $service"
        $compose_cmd logs -f --tail=100 "$service"
    else
        log_info "Tailing logs for all services"
        $compose_cmd logs -f --tail=50
    fi
}

cmd_trigger_pipeline() {
    log_header "Triggering Pipeline Run"

    local orchestrator_port="${SERVICE_PORTS["orchestrator"]}"
    local url="http://localhost:${orchestrator_port}/pipeline/run"

    log_info "Sending pipeline trigger to orchestrator..."

    response=$(curl -s -X POST "$url" \
        -H "Content-Type: application/json" \
        -d '{}' \
        --connect-timeout 5 \
        --max-time 10 \
        2>/dev/null) || {
        log_error "Failed to reach orchestrator at $url"
        log_info "Make sure services are running: ./run_services.sh --status"
        exit 1
    }

    log_success "Pipeline triggered!"
    echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
}

cmd_trigger_crawl() {
    log_header "Triggering Crawl"

    local crawler_port="${SERVICE_PORTS["crawler"]}"
    local url="http://localhost:${crawler_port}/crawl/trigger"

    log_info "Sending crawl trigger..."

    response=$(curl -s -X POST "$url" \
        -H "Content-Type: application/json" \
        -d '{}' \
        --connect-timeout 5 \
        --max-time 10 \
        2>/dev/null) || {
        log_error "Failed to reach crawler at $url"
        exit 1
    }

    log_success "Crawl triggered!"
    echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
}

cmd_trigger_notify() {
    log_header "Triggering Notification Run"

    local orchestrator_port="${SERVICE_PORTS["orchestrator"]}"
    local url="http://localhost:${orchestrator_port}/pipeline/run"

    log_info "Sending notification-only trigger to orchestrator..."
    log_info "(skip_crawl=true, skip_extraction=true, skip_llm=true)"

    response=$(curl -s -X POST "$url" \
        -H "Content-Type: application/json" \
        -d '{"skip_crawl": true, "skip_extraction": true, "skip_llm": true}' \
        --connect-timeout 5 \
        --max-time 10 \
        2>/dev/null) || {
        log_error "Failed to reach orchestrator at $url"
        log_info "Make sure services are running: ./run_services.sh --status"
        exit 1
    }

    log_success "Notification run triggered!"
    echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
}

cmd_trigger_weekly_report() {
    log_header "Triggering Weekly Feedback Report"

    local orchestrator_port="${SERVICE_PORTS["orchestrator"]}"
    local url="http://localhost:${orchestrator_port}/pipeline/weekly-report"

    log_info "Generating the weekly top-articles PDF report..."
    log_info "The report will be delivered to all configured Slack channels."

    response=$(curl -s -X POST "$url" \
        -H "Content-Type: application/json" \
        -d '{}' \
        --connect-timeout 10 \
        --max-time 120 \
        2>/dev/null) || {
        log_error "Failed to reach orchestrator at $url"
        log_info "Make sure services are running: ./run_services.sh --status"
        exit 1
    }

    log_success "Weekly report request completed!"
    echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
}

cmd_trigger_test_notification() {
    log_header "Triggering Dummy Notification Test"

    local orchestrator_port="${SERVICE_PORTS["orchestrator"]}"
    local url="http://localhost:${orchestrator_port}/pipeline/test-notification"

    log_info "Sending dummy articles to #research-general for end-to-end testing..."
    log_info "This will post test messages to Slack and verify article/slack_message rows in ScyllaDB."

    response=$(curl -s -X POST "$url" \
        -H "Content-Type: application/json" \
        -d '{"count": 2, "channel": "#research-general", "title_prefix": "[TEST]"}' \
        --connect-timeout 10 \
        --max-time 120 \
        2>/dev/null) || {
        log_error "Failed to reach orchestrator at $url"
        log_info "Make sure services are running: ./run_services.sh --status"
        exit 1
    }

    log_success "Dummy notification test completed!"
    echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
}

usage() {
    echo -e "${CYAN}Preventive Health Research Pipeline - Service Manager${NC}"
    echo ""
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  (default)             Start all core services"
    echo "  --start               Start all core services"
    echo "  --stop                Stop all services"
    echo "  --restart             Restart all services"
    echo "  --status              Show container status (docker-compose ps)"
    echo "  --health              Run health checks on all services"
    echo "  --logs [SERVICE]      Tail logs (optionally for a specific service)"
    echo "  --build               Rebuild images and start all services"
    echo "  --trigger-pipeline    Trigger a full pipeline run via orchestrator"
    echo "  --trigger-crawl       Trigger a crawl run via crawler service"
    echo "  --trigger-notify      Trigger notification-only run (skip crawl/extraction/llm)"
    echo "  --trigger-weekly-report Trigger the weekly PDF report"
    echo "  --trigger-test-notification Send dummy articles to #research-general and verify DB storage"
    echo "  --help                Show this help message"
    echo ""
    echo "Options:"
    echo "  --scaled              Include extra extraction worker (profile: scaled)"
    echo "  --services SVC1,SVC2  Start/restart only specific services"
    echo ""
    echo "Services: crawler, dedup, extraction-worker-1, extraction-worker-2, llm, notification, orchestrator, api-gateway"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Start all core services"
    echo "  $0 --build --scaled                   # Rebuild and start all including scaled workers"
    echo "  $0 --services crawler,llm             # Start only crawler and llm"
    echo "  $0 --restart --services notification  # Restart notification service"
    echo "  $0 --logs crawler                     # Tail crawler logs"
    echo "  $0 --trigger-pipeline                 # Trigger manual pipeline run"
    echo "  $0 --trigger-notify                   # Trigger notification-only run"
    echo "  $0 --trigger-weekly-report            # Generate the weekly feedback PDF report"
    echo "  $0 --trigger-test-notification        # Post dummy Slack articles and verify ScyllaDB storage"
    echo "  $0 --stop                             # Stop everything"
}

# ============================================================
# Main
# ============================================================

main() {
    check_prerequisites

    local command="start"
    local services=""
    local build_flag=""
    local profile_flag=""
    local log_service=""

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --start)
                command="start"
                shift
                ;;
            --stop)
                command="stop"
                shift
                ;;
            --restart)
                command="restart"
                shift
                ;;
            --status)
                command="status"
                shift
                ;;
            --health)
                command="health"
                shift
                ;;
            --logs)
                command="logs"
                shift
                if [[ $# -gt 0 && ! "$1" =~ ^-- ]]; then
                    log_service="$1"
                    shift
                fi
                ;;
            --build)
                build_flag="build"
                shift
                ;;
            --scaled)
                profile_flag="scaled"
                shift
                ;;
            --services)
                shift
                if [[ $# -gt 0 ]]; then
                    services=$(echo "$1" | tr ',' ' ')
                    shift
                else
                    log_error "--services requires a comma-separated list"
                    exit 1
                fi
                ;;
            --trigger-pipeline)
                command="trigger-pipeline"
                shift
                ;;
            --trigger-crawl)
                command="trigger-crawl"
                shift
                ;;
            --trigger-notify)
                command="trigger-notify"
                shift
                ;;
            --trigger-weekly-report)
                command="trigger-weekly-report"
                shift
                ;;
            --trigger-test-notification)
                command="trigger-test-notification"
                shift
                ;;
            --help|-h)
                usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done

    case "$command" in
        start)
            cmd_start "$services" "$build_flag" "$profile_flag"
            ;;
        stop)
            cmd_stop
            ;;
        restart)
            cmd_restart "$services" "$profile_flag"
            ;;
        status)
            cmd_status
            ;;
        health)
            cmd_health
            ;;
        logs)
            cmd_logs "$log_service"
            ;;
        trigger-pipeline)
            cmd_trigger_pipeline
            ;;
        trigger-crawl)
            cmd_trigger_crawl
            ;;
        trigger-notify)
            cmd_trigger_notify
            ;;
        trigger-weekly-report)
            cmd_trigger_weekly_report
            ;;
        trigger-test-notification)
            cmd_trigger_test_notification
            ;;
    esac
}

main "$@"
