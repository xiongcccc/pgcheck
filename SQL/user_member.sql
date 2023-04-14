WITH RECURSIVE x AS (
    SELECT
        member::regrole,
        roleid::regrole AS role,
        member::regrole || ' -> ' || roleid::regrole AS path
    FROM
        pg_auth_members AS m
    WHERE
        roleid > 16384
    UNION ALL
    SELECT
        x.member::regrole,
        m.roleid::regrole,
        x.path || ' -> ' || m.roleid::regrole
    FROM
        pg_auth_members AS m
        JOIN x ON m.member = x.role
)
SELECT
    member,
    ROLE,
    path
FROM
    x
ORDER BY
    member::text,
    ROLE::text;
