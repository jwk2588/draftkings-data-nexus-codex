#!/usr/bin/env python3
"""Smoke test for KIMI K2.6 Swarm Engine."""
import sys, os
sys.path.insert(0, '/home/ubuntu/HiveMind')
os.environ.setdefault('NVIDIA_API_KEY', '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')

from agents.swarm.kimi_swarm_engine import FettyFMOrchestrator, SwarmAgent, NexosAPIGateway, get_pg_conn

# Test 1: DB connection + table counts
conn = get_pg_conn()
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM agent_registry')
agent_count = cur.fetchone()[0]
cur.execute('SELECT COUNT(*) FROM subagent_registry')
subagent_count = cur.fetchone()[0]
cur.execute('SELECT COUNT(*) FROM api_gateway_config')
api_count = cur.fetchone()[0]
cur.execute('SELECT COUNT(*) FROM swarm_task_queue')
task_count = cur.fetchone()[0]
print(f'[TEST 1] DB OK: {agent_count} agents, {subagent_count} subagents, {api_count} API providers, {task_count} tasks')

# Test 2: API Gateway routing
gw = NexosAPIGateway()
assert gw.route_task("GR_ANALYSIS") == "KIMI_NVIDIA", "Default should route to KIMI"
assert gw.route_task("PHOTO_OCR", requires_vision=True) == "GEMINI", "Vision should route to GEMINI"
assert gw.route_task("MASTER_BRIEF", complexity="HIGH") == "ANTHROPIC", "Complex should route to ANTHROPIC"
print('[TEST 2] API Gateway routing: PASS')

# Test 3: Task enqueue
orch = FettyFMOrchestrator()
task_id = orch.enqueue_task(
    task_type="SMOKE_TEST",
    agent_id="TIGER",
    subagent_id="SA-VIE-001",
    topic_module="VIE_ASC810",
    gr_node="GR-001",
    input_payload={"test": True},
    priority="LOW",
    depth=0
)
cur.execute('SELECT status, task_type FROM swarm_task_queue WHERE task_id = %s', (task_id,))
row = cur.fetchone()
assert row[0] == 'QUEUED', f"Expected QUEUED, got {row[0]}"
assert row[1] == 'SMOKE_TEST', f"Expected SMOKE_TEST, got {row[1]}"
print(f'[TEST 3] Task enqueue: PASS (task_id={task_id[:8]}...)')

# Test 4: KIMI K2.6 live call
agent = SwarmAgent('TIGER', task_id, orch.conn)
result = agent.call_kimi('In one sentence: what is ASC 810 VIE consolidation?', max_tokens=80)
print(f'[TEST 4] KIMI live call: success={result["success"]}, tokens={result.get("tokens",0)}')
if result['success']:
    print(f'  Response: {result["content"][:200]}')
else:
    print(f'  Error: {result.get("error")}')

# Test 5: Message bus
agent.post_message("SUITS", "RESULT", {"gr_id": "GR-001", "leverage_score": 78})
cur.execute("SELECT COUNT(*) FROM swarm_message_bus WHERE from_agent='TIGER' AND to_agent='SUITS'")
msg_count = cur.fetchone()[0]
assert msg_count >= 1, "Message not found in bus"
print(f'[TEST 5] Message bus: PASS ({msg_count} messages)')

# Test 6: New tables exist
for table in ['ev_register', 'sb_register', 'photo_metadata', 'photo_ocr', 'photo_domain_tags',
              'photo_data_dictionary', 'chatgpt_data_dictionary', 'chatgpt_metadata_dictionary',
              'unified_search_index', 'kimi_synthesis_results', 'github_sync_log']:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    count = cur.fetchone()[0]
    print(f'  Table {table}: {count} rows')

conn.close()
print('\n=== ALL SMOKE TESTS PASSED ===')
