----------------------------------------------------------------------------
--See full blog at:
--https://crunchydata.com/blog/vehicle-routing-with-postgis-and-overture-data

-- OVERTURE PGROUTING
--

SET pgaudit.log TO 'none';
CREATE EXTENSION crunchy_spatial_analytics CASCADE;
CREATE EXTENSION pgrouting;
RESET pgaudit.log;

CREATE FOREIGN TABLE ov_places ()
    SERVER crunchy_lake_analytics
    OPTIONS (path 's3://overturemaps-us-west-2/release/2024-08-20.0/theme=places/type=*/*.parquet');

CREATE FOREIGN TABLE ov_segments ()
    SERVER crunchy_lake_analytics
    OPTIONS (
        path 's3://overturemaps-us-west-2/release/2024-08-20.0/theme=transportation/type=segment/*.parquet');

CREATE FOREIGN TABLE ov_connectors ()
    SERVER crunchy_lake_analytics
    OPTIONS (path 's3://overturemaps-us-west-2/release/2024-08-20.0/theme=transportation/type=connector/*.parquet');


--
-- Check out what is in the FDW tables
--
-- use row_to_json() to surface the structure of the 
-- nested data elements a little better than a raw * does
--
SELECT row_to_json(ov_segments, true) 
  FROM ov_segments 
  LIMIT 1 OFFSET 100;

SELECT row_to_json(ov_connectors, true) 
  FROM ov_connectors 
  LIMIT 1 OFFSET 100;

SELECT row_to_json(ov_places, true) 
  FROM ov_places 
  LIMIT 1 OFFSET 100;


--
-- Deal with kmph/mph units, and fill in any null 
-- speed information with sensible defaults based 
-- on the segment class.
--
CREATE OR REPLACE FUNCTION pgr_segment_kmph(speed float8, unit text, class text)
RETURNS FLOAT8 AS
$$
DECLARE
    default_kmph FLOAT8 := 40;
BEGIN

    -- Convert mph to kmph where necessary
    IF unit = 'mph' THEN
        speed := speed * 1.60934;
    END IF;

    IF speed IS NOT NULL THEN
        RETURN speed;
    END IF;

    -- Apply some defaults
    -- Should not be driving fast on service roads
    IF class = 'service' THEN
        speed := 20;
    -- Or on residential roads
    ELSIF class = 'residential' THEN
        speed := 30;
    -- Everywhere else, use the default
    ELSE 
        speed := coalesce(speed, default_kmph);
    END IF;

    RETURN speed;

END;
$$ LANGUAGE 'plpgsql';

--
-- The cost to traverse a segment is the number of 
-- seconds needed to traverse it, so distance over speed.
-- 
CREATE OR REPLACE FUNCTION pgr_segment_cost(geom geometry, speed_kmph float8)
RETURNS FLOAT8 AS
$$
DECLARE
    length_meters FLOAT8;
    default_kmph FLOAT8 := 40;
    kmph FLOAT8;
    cost FLOAT8;
    meters_per_second FLOAT8;
BEGIN
    -- Geography length is in meters
    length_meters := ST_Length(geom::geography);

    -- Convert km/hour into meters/second
    meters_per_second := speed_kmph * 1000.0 / 3600.0;

    -- Segment cost is the number of seconds 
    -- needed to traverse the segment
    cost := length_meters / meters_per_second;

    RETURN cost;
END;
$$ LANGUAGE 'plpgsql';


--
-- Create a simple table that reflects some of the 
-- input we have generated (speed, directionality)
-- and just mirrors some other useful info (surface,
-- primary name) for mapping purposes. 
-- Most importantly, carry out the chopping of segments
-- into edges with only two graph connectors, one at
-- the start and one at the end.
-- 
CREATE OR REPLACE FUNCTION ov_to_pgr(segment ov_segments)
RETURNS TABLE(
    id text,
    geometry geometry(LineString, 4326),
    connector_source text,
    connector_target text,
    class text,
    subclass text,
    surface text,
    speed_kmph real,
    primary_name text,
    one_way boolean
) AS
$$
DECLARE
    n integer;
    connector_to float8;
    connector_from float8 := 0.0;
BEGIN

    -- Carry over some attributes directly
    id := segment.id;
    class := segment.class;
    subclass := segment.subclass;
    primary_name := (segment.names).primary;
    -- Take the first surface we see rather than 
    -- chopping up the segment here
    surface := segment.road_surface[1].value;

    speed_kmph := pgr_segment_kmph(segment.speed_limits[1].max_speed.value, segment.speed_limits[1].max_speed.unit, segment.class);
    
    -- Most edges are two-way, but a few are one-way, flag 
    -- those so we can adjust the cost later
    one_way := false;
    IF segment.access_restrictions IS NOT NULL THEN

        -- Overture uses "backward" access restrictions 
        -- for one-way segments, and the restriction can
        -- show up anywhere in the list, so...
        n := array_length(segment.access_restrictions, 1);
        FOR i IN 1..n LOOP
            IF segment.access_restrictions[i].access_type = 'denied' AND segment.access_restrictions[i].when.heading = 'backward' THEN
                one_way := true;
                EXIT;
            END IF;
        END LOOP;
    END IF;

    -- Chop segments into edges with vertexes at 
    -- the connectors. Each edge has two connectors
    -- (one at each end) so a list of 3 connectors
    -- implies outputting 2 edges.
    connector_target := segment.connectors[1].connector_id;
    connector_to := 0.0;
    n := array_length(segment.connectors, 1);
    FOR i IN 2..n LOOP

        -- Avoid emitting zero-length segments
        IF connector_to = segment.connectors[i].at THEN
            CONTINUE;
        END IF;
        connector_from := connector_to;
        connector_source := connector_target;
        connector_to := segment.connectors[i].at;
        connector_target := segment.connectors[i].connector_id;

        -- This is where we chop!
        geometry := ST_SetSRID(ST_LineSubstring(segment.geometry, connector_from, connector_to),4326);

        -- Table-valued output means the return fills
        -- in the output parameters for us magically,
        -- as long as we have used the correct variable
        -- names. 
        RETURN NEXT;
    END LOOP;

END;
$$ LANGUAGE 'plpgsql';


--
-- A data processing function that will download transportation
-- segments using the bounding box of the geometry, and then
-- process that raw data into tables ready to route using 
-- pgRouting
--
CREATE OR REPLACE FUNCTION ov_download(extent GEOMETRY)
RETURNS boolean AS
$$
DECLARE
    nrows integer;
BEGIN

    --
    -- Create a local copy of the Overture data
    -- so that it is easy to debug the attributes
    -- of particular features (it is very very very 
    -- slow to directly pull one feature out of the
    -- FDW tables with an id filter, because the 
    -- geoparquet files are not sorted by id)
    --
    
    DROP TABLE IF EXISTS ov_segments_local;

    RAISE NOTICE 'Creating ov_segments_local...';
    CREATE TABLE ov_segments_local () 
        INHERITS (ov_segments);

    INSERT INTO ov_segments_local
          SELECT * 
        FROM ov_segments
        WHERE (bbox).xmin >= ST_XMin(extent)
        AND (bbox).xmax <= ST_XMax(extent)
        AND (bbox).ymin >= ST_YMin(extent)
        AND (bbox).ymax <= ST_YMax(extent);

    -- Get the number of rows affected
    GET DIAGNOSTICS nrows = ROW_COUNT;
    RAISE NOTICE '  % rows copied', nrows;

    --
    -- Chop segments into edges, using the connectors list
    -- and proportions. Extract other attributes like name
    -- surface, speed and so on for easier access.
    --
    RAISE NOTICE 'Creating pgr_segments...';
    DROP TABLE IF EXISTS pgr_segments;
    CREATE TABLE pgr_segments AS
        SELECT (ov_to_pgr(ov_segments_local)).*
        FROM ov_segments_local;

    GET DIAGNOSTICS nrows = ROW_COUNT;
    RAISE NOTICE '  % rows created', nrows;

    --
    -- Extract the connector geometry from the connectors
    -- list, materializing a new connector for each proportion.
    -- Create an integer unique id for each connector while
    -- where are at it.
    --
    DROP SEQUENCE IF EXISTS pgr_connector_seq;
    CREATE SEQUENCE pgr_connector_seq;

    RAISE NOTICE 'Creating pgr_connectors...';
    DROP TABLE IF EXISTS pgr_connectors;
    CREATE TABLE pgr_connectors AS 
        WITH connectors AS (
            SELECT (unnest(connectors)).*, geometry 
            FROM ov_segments_local 
            WHERE class IN ('motorway', 'primary', 'residential', 'secondary', 'tertiary', 'trunk', 'unclassified')
        )
        SELECT DISTINCT ON (connector_id) 
            nextval('pgr_connector_seq') AS vertex_id, 
            connector_id, 
            ST_SetSRID(ST_LineInterpolatePoint(geometry, at),4326)::geometry(point, 4326) AS geometry 
        FROM connectors;

    GET DIAGNOSTICS nrows = ROW_COUNT;
    RAISE NOTICE '  % rows created', nrows;

    CREATE INDEX pgr_connectors_x ON pgr_connectors (connector_id);
    CREATE INDEX pgr_connectors_geom_x ON pgr_connectors USING GIST (geometry);

    --
    -- Get the integer ids for the source/target connectors
    -- for each edge, and add an integer edge id while we are 
    -- at it. Convert speed/length into cost, and ensure
    -- one-way edges have negative cost.
    --

    DROP SEQUENCE IF EXISTS pgr_segment_seq;
    CREATE SEQUENCE pgr_segment_seq;

    RAISE NOTICE 'Creating pgr_edges...';
    DROP TABLE IF EXISTS pgr_edges;
    CREATE TABLE pgr_edges AS
    SELECT 
        -- Assign a bigint unique id to the edge
        nextval('pgr_segment_seq') AS edge_id, 

        -- Lookup the bigint unique id for the source/target vertices
        c1.vertex_id AS source_vertex_id,
        c2.vertex_id AS target_vertex_id,

        -- Calculate the cost
        pgr_segment_cost(seg.geometry, seg.speed_kmph) AS cost,

        -- Infinite cost when trying to travel a one-way street backwards
        CASE WHEN one_way THEN -1 ELSE pgr_segment_cost(seg.geometry, seg.speed_kmph) END AS reverse_cost,

        -- Will need the edge geometry to construct the spatial path
        -- from the route edge sequence
        ST_SetSRID(seg.geometry, 4326)::geometry(Geometry, 4326) AS geometry,
        ST_Length(seg.geometry::geography) AS length,

        -- Carry over info from the overture tables
        seg.id,
        seg.connector_source, 
        seg.connector_target, 
        seg.class,   
        seg.subclass,         
        seg.surface,          
        seg.speed_kmph,       
        seg.primary_name,     
        seg.one_way    
    FROM pgr_segments seg
    LEFT JOIN pgr_connectors c1 ON c1.connector_id = seg.connector_source
    LEFT JOIN pgr_connectors c2 ON c2.connector_id = seg.connector_target
    WHERE seg.class IN ('motorway', 'primary', 'residential', 'secondary', 'tertiary', 'trunk', 'unclassified')
      AND c1.connector_id IS NOT NULL 
      AND c2.connector_id IS NOT NULL;

    GET DIAGNOSTICS nrows = ROW_COUNT;
    RAISE NOTICE '  % rows created', nrows;

    CREATE INDEX pgr_edges_geom_x ON pgr_edges USING GIST (geometry);
    CREATE INDEX pgr_edges_id_x ON pgr_edges (edge_id);

    RAISE NOTICE 'Done.';
    RETURN true;

END;
$$ LANGUAGE 'plpgsql';

--
-- Run the download and prep process! Uses the extent of the
-- geometry as the bounding box, so a diagonal linestring
-- is an easy way to define a box. This bounding box surrounds
-- Victoria, Canada.
--
SELECT ov_download('LINESTRING(-123.45466 48.39195,-123.28350 48.52226)');



--
-- Ready to route!
-- Very simple implemenation of a routing function.
--

CREATE OR REPLACE FUNCTION pgr_routeline(pt0 geometry, pt1 geometry)
RETURNS TEXT AS
$$
DECLARE
    vertex0 bigint;
    vertex1 bigint;
    edges_sql text;
    result text;
BEGIN

    -- Lookup the nearest vertex to our start and end geometry
    SELECT vertex_id INTO vertex0 FROM pgr_connectors ORDER BY geometry <-> pt0 LIMIT 1;
    SELECT vertex_id INTO vertex1 FROM pgr_connectors ORDER BY geometry <-> pt1 LIMIT 1;
    RAISE DEBUG 'vertex0=% vertex1=%', vertex0, vertex1;

    --
    -- SQL to create a pgRouting graph
    -- This is as simple as they come. 
    -- More complex approaches might
    --  * scale cost based on class
    --  * restrict edges based on box formed
    --    by start/end points
    --  * restrict edges based on class
    --
    edges_sql := 'SELECT 
            edge_id AS id, 
            source_vertex_id AS source, 
            target_vertex_id AS target, 
            cost, reverse_cost
        FROM pgr_edges';

    -- Run the Dijkstra shortest path and join back to edges
    -- to create the path geometry
    SELECT ST_AsGeoJSON(ST_Union(e.geometry))
        INTO result 
        FROM pgr_dijkstra(edges_sql, vertex0, vertex1) pgr
        JOIN pgr_edges e
        ON e.edge_id = pgr.edge;

    RETURN result;

END;
$$ LANGUAGE 'plpgsql';

--
-- Run the routing function, with two points
SELECT pgr_routeline(
    ST_Point(-123.37826,48.41976, 4326),
    ST_Point(-123.35214,48.43891, 4326));


--
-- Done!
--

