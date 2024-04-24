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


-- create views
CREATE MATERIALIZED VIEW user_profile_view AS
SELECT
    u.id as user_id,
    u.displayname,
    u.creationdate,
    u.aboutme,
    u.websiteurl,
    u.location,
    u.reputation,
    array_agg(DISTINCT b.name) AS badges
FROM
    users u
LEFT JOIN
    badges b ON u.id = b.userid
GROUP BY
    u.id,
    u.displayname,
    u.creationdate,
    u.aboutme,
    u.websiteurl,
    u.location,
    u.reputation;


CREATE MATERIALIZED VIEW get_question_view AS
SELECT ql.questionid as q_id, json_agg(json_build_object('question', ql.relatedquestionid, 'type', ql.linktypeid)) as links_list
FROM questionslinks ql
GROUP BY ql.questionid;

-- create triggers

CREATE OR REPLACE FUNCTION refresh_get_question_view()
RETURNS TRIGGER AS
$$
BEGIN
    REFRESH MATERIALIZED VIEW get_question_view;
    RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER trigger_refresh_get_question_view
AFTER INSERT OR UPDATE OR DELETE ON questionslinks
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_get_question_view();




-- create triggers

CREATE OR REPLACE FUNCTION refresh_user_profile_view()
RETURNS TRIGGER AS
$$
BEGIN
    REFRESH MATERIALIZED VIEW user_profile_view;
    RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER trigger_refresh_user_profile_view
AFTER INSERT OR UPDATE OR DELETE ON users
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_user_profile_view();

CREATE TRIGGER trigger_refresh_user_profile_view_badges
AFTER INSERT OR UPDATE OR DELETE ON badges
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_user_profile_view();


-- create indexes
CREATE INDEX idx_user_id ON user_profile_view (user_id) ;
CREATE INDEX idx_question_id ON get_question_view (q_id) ;