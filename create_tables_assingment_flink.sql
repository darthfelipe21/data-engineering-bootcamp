-- DDL for main session events table
-- Stores user session details: start, end, IP, and host
CREATE TABLE assignment_events_sessions (
    session_start TIMESTAMP(3), 
    session_end TIMESTAMP(3),   
    ip VARCHAR,                 
    host VARCHAR                
);

-- DDL for session events table with referrer
-- Extends previous table, adds referrer
CREATE TABLE processed_assignment_events_source_sessions (
    session_start TIMESTAMP(3), 
    session_end TIMESTAMP(3),   
    ip VARCHAR,                 
    host VARCHAR,               
    referrer VARCHAR            
);


SELECT * FROM assignment_events_sessions WHERE host LIKE '%techcreator%';

SELECT * FROM processed_assignment_events_source_sessions WHERE host LIKE '%techcreator%';

