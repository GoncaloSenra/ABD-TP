-- update the sequences
SELECT setval('answers_id_seq', (SELECT MAX(id) FROM Answers));
SELECT setval('comments_id_seq', (SELECT MAX(id) FROM Comments));
SELECT setval('questions_id_seq', (SELECT MAX(id) FROM Questions));
SELECT setval('tags_id_seq', (SELECT MAX(id) FROM Tags));
SELECT setval('users_id_seq', (SELECT MAX(id) FROM Users));
SELECT setval('votes_id_seq', (SELECT MAX(id) FROM Votes));


-- create indexes
CREATE INDEX idx_questions_title_fts ON questions USING GIN(to_tsvector('english', title)); 
CREATE INDEX idx_questions_creationdate ON questions (creationdate);
CREATE INDEX idx_postid ON votes(postid);
CREATE INDEX idx_tags_questionid ON questionstags (questionid);
CREATE INDEX idx_answers_parentid ON answers (parentid);
CREATE INDEX idx_questionslinks_questionid ON questionslinks (questionid);
