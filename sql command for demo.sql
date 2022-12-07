-- SELECT ALL CANDIDATES
SELECT * FROM candidates.candidates_table;
-- RESULT FOR 1: FILTER LOCATION
SELECT * FROM candidates.filter_location_demo;
SELECT * FROM candidates.candidates_table WHERE id = 2767;
SELECT * FROM candidates.jobs_table where id in (41330,59765,59770,60944,62270,62271,62481,63316,63317,63372,63396,63560,41152);
-- RESULT FOR 2: FILTER FUNCTION
SELECT * FROM candidates.filter_desire_demo; -- Result in table
SELECT * FROM candidates.candidates_table WHERE id = 2766; -- Information for candidate 2766
SELECT * FROM candidates.jobs_table where id in (59751,59957,62016,62271,62785,62786,62787,62788,62789,62790,63299,63302,63303,63304,63305,63306,63307,63387,63556,63560,63561,63562,67542,70315,70591,71651);
-- RESULT FOR 3: FILTER CAUSE
SELECT * FROM candidates.filter_cause_demo;
SELECT * FROM candidates.candidates_table WHERE id = 2764;
SELECT * FROM candidates.jobs_table where id in (41152,41161,41327,41328,41329,41330,41339,41348,41428,42163,42219,42493,42712,42848,42924,43096,43100,43104,43115,43132,43170,43175,43176);
-- RESULT FOR 4: FILTER 3 CONDIDTIONS
SELECT * FROM candidates.filter_3conditions_demo;
SELECT * FROM candidates.candidates_table WHERE id = 2768;
SELECT * FROM candidates.jobs_table where id in (63514, 54342, 63373, 51561, 63533, 45379, 63484, 63530, 44684, 63524, 63374, 42712, 58721, 63526, 57508, 54400, 63488, 63534, 63522, 63527, 58141, 48687, 45713, 42924, 43132, 63079, 63487, 59758, 48649, 51523, 63529, 63486, 53941, 63375, 58083);
-- RESULT FOR 5: Get experience year for each candidate
SELECT * FROM candidates.candidates_exp_demo;
-- RESULT FOR 6: Update Candidate From Hassan's Tables
SELECT * FROM candidates.candidates_table;
UPDATE candidates.candidates_table SET create_date = "2022-12-06 19:32:11" WHERE id IN(2764,2765,2766,2767);
SELECT * FROM candidates.filter_3conditions_demo;
-- RESULT FOR 7: Parsing skills
SELECT * FROM candidates.candidates_skills_demo;
-- RESULT FOR 8: Ranking job function
SELECT * FROM candidates.candidates_ranking_demo;

