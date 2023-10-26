staging_dataset = """
CREATE SCHEMA IF NOT EXISTS {};
"""

dim_all_calls = """

CREATE TABLE IF NOT EXISTS {}.dim_all_calls(
id INTEGER,
call_id INTEGER,
caller_id VARCHAR,
agent_id INTEGER,
call_type VARCHAR,
assigned_agent_id INTEGER,
call_duration_in_seconds INTEGER,
call_ended_by_agent BOOLEAN

);
"""
dim_recieved_calls = """
CREATE TABLE IF NOT EXISTS {}.dim_recieved_calls(
id INTEGER,
recieved_call_id INTEGER,
inbound_caller_id VARCHAR,
call_duration_in_seconds INTEGER,
recieving_agent_id INTEGER,
assigned_agent_id INTEGER

);
"""
dim_assigned_calls = """
CREATE TABLE IF NOT EXISTS {}.dim_assigned_calls(
id INTEGER,
call_id INTEGER,
caller_id VARCHAR,
assigned_calls_to_agent_id INTEGER,
complaint_status VARCHAR
);
"""
dim_resolved_calls = """
CREATE TABLE IF NOT EXISTS {}.dim_resolved_calls(
id INTEGER,
resolved_call_id INTEGER,
caller_id VARCHAR,
recieving_agent_id INTEGER,
assigned_agent_id INTEGER,
complaint_status VARCHAR,
resolution_duration_in_hours INTEGER
);
"""

dim_complaints = """
CREATE TABLE IF NOT EXISTS {}.dim_complaints(
id INTEGER,
call_id INTEGER,
complaint_topic VARCHAR,
complaint_status VARCHAR

);
"""
dim_agents = """
CREATE TABLE IF NOT EXISTS {}.dim_agents(
id INTEGER,
agent_id INTEGER,
agents_grade_level VARCHAR

);
"""

fact_call_resolution = """
CREATE TABLE IF NOT EXISTS {}.fact_call_resolution(
all_calls_id INTEGER,
all_calls_call_id INTEGER,
all_callers_id VARCHAR,
assigned_id INTEGER,
assigned_call_id INTEGER,
resolved_id INTEGER,
resolved_calls_id INTEGER,
complaint_id INTEGER,
resolved_agent_id INTEGER,
recieved_call_id INTEGER,
agent_id INTEGER,
resolution_duration_in_hours INTEGER,
call_duration_in_seconds INTEGER

)
"""

transform_tables = [dim_all_calls, dim_recieved_calls, dim_assigned_calls,
                            dim_resolved_calls, dim_complaints, dim_agents, fact_call_resolution]


