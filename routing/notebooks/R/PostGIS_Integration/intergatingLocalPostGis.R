require(RPostgreSQL)
m <- dbDriver("PostgreSQL")
con <- dbConnect(m, dbname="hellogis")
q="SELECT
ST_Distance(ST_GeomFromText('POINT(-87.6348345 41.8786207)', 4326), landmarks.the_geom) AS planar_degrees,
name,
architect
FROM landmarks
ORDER BY planar_degrees ASC
LIMIT 5;"
rs = dbSendQuery(con,q)
df = fetch(rs,n=-1)
