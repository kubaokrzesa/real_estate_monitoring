
survey_tab_drop = "DROP TABLE IF EXISTS surveys"

survey_tab_creation = """
CREATE TABLE IF NOT EXISTS surveys
(
    survey_id TEXT UNIQUE,
    survey_date DATE,
    survey_type TEXT,
    location TEXT,
    n_pages INT
)

"""

survey_table_insert = """INSERT INTO surveys (survey_id, survey_date, survey_type, location, n_pages) 
                 VALUES (?, ?, ?, ?, ?);"""


survey_links_tab_creation = """
CREATE TABLE IF NOT EXISTS survey_links
(
    survey_id TEXT,
    link TEXT,
    UNIQUE(survey_id, link)
)

"""