raw_dataset = """
CREATE SCHEMA IF NOT EXISTS {};
"""

agents = """
CREATE TABLE IF NOT EXISTS {}.agents(
id INTEGER,
recieving_agent_id INTEGER,
call_id INTEGER,
call_type VARCHAR,
call_duration_in_seconds INTEGER,
agents_grade_level VARCHAR
);
"""

all_calls = """
CREATE TABLE IF NOT EXISTS {}.all_calls(
id INTEGER,
call_id INTEGER,
caller_id VARCHAR,
recieving_agent_id INTEGER,
assigned_agent_id INTEGER,
call_duration_in_seconds INTEGER,
call_type VARCHAR,
call_ended_by_agent BOOLEAN

);
"""
recieved_calls = """
CREATE TABLE IF NOT EXISTS {}.recieved_calls(
id INTEGER,
call_id INTEGER,
caller_id VARCHAR,
call_type VARCHAR,
complaint_topic VARCHAR,
call_duration_in_seconds INTEGER,
recieving_agent_id INTEGER,
assigned_agent_id INTEGER
);
"""

resolved_calls = """
CREATE TABLE IF NOT EXISTS {}.resolved_calls(
id INTEGER,
call_id INTEGER,
caller_id VARCHAR,
recieving_agent_id INTEGER,
complaint_topic VARCHAR,
complaint_status VARCHAR,
assigned_agent_id INTEGER,
resolution_duration_in_hours INTEGER

);
"""
assigned_calls = """
CREATE TABLE IF NOT EXISTS {}.assigned_calls(
id INTEGER,
call_id INTEGER,
caller_id VARCHAR,
assigned_calls_to_agent_id INTEGER,
complaint_status VARCHAR
);
"""

complaints = """
CREATE TABLE IF NOT EXISTS {}.complaints(
id INTEGER,
call_id INTEGER,
complaint_topic VARCHAR,
complaint_status VARCHAR
);
"""
raw_tables = [all_calls, agents,
                      recieved_calls, resolved_calls, assigned_calls, complaints]
