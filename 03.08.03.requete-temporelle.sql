-- Requête à une date précise (équivalent FOR SYSTEM_TIME AS OF)
SELECT *
FROM get_joconde_as_of('2024-01-01 00:00:00'::TIMESTAMPTZ)
WHERE reference = '00000055011';