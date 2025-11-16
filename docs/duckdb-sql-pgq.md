SQL/PGQ

SQL/PGQ is a graph query language built on top of SQL, designed to bring graph pattern matching capabilities to both seasoned SQL users and those new to graph technology. Standardized by the International Organization for Standardization (ISO), it offers a declarative approach to querying property graphs, which store nodes, edges, and properties.

The language features a visual graph syntax inspired by Cypher while also supporting traditional SQL syntax, easing the transition for SQL users. With SQL/PGQ, you can query property graphs to:

    Discover paths between nodes
    Identify specific graph patterns
    Calculate the shortest path between two nodes

See here for a list of resources related to SQL/PGQ.
Loading data

Starting with an empty DuckDB database, load the Person and Person_knows_person tables from the LDBC SNB dataset using the following commands:

CREATE TABLE Person AS SELECT _ FROM 'https://gist.githubusercontent.com/Dtenwolde/2b02aebbed3c9638a06fda8ee0088a36/raw/8c4dc551f7344b12eaff2d1438c9da08649d00ec/person-sf0.003.csv';
CREATE TABLE Person_knows_person AS SELECT _ FROM 'https://gist.githubusercontent.com/Dtenwolde/81c32c9002d4059c2c3073dbca155275/raw/8b440e810a48dcaa08c07086e493ec0e2ec6b3cb/person_knows_person-sf0.003.csv';

Creating the property graph

Next, create a property graph, which is persistent across database sessions and automatically reflects changes made to the underlying data. Similar to a VIEW, the property graph provides a layer for querying graph structures, ensuring that updates to the base tables are immediately reflected in the graph representation. For more details, refer to Property graph.

Use the following command to define the property graph:

CREATE PROPERTY GRAPH snb
VERTEX TABLES (
Person
)
EDGE TABLES (
Person_knows_person
SOURCE KEY ( person1id ) REFERENCES Person ( id )
DESTINATION KEY ( person2id ) REFERENCES Person ( id )
LABEL Knows
);

If successful, you will see the following confirmation, allowing you to execute queries using SQL/PGQ syntax on the created property graph:

┌─────────┐
│ Success │
│ boolean │
├─────────┤
│ 0 rows │
└─────────┘

Pattern matching queries

SQL/PGQ uses a visual graph syntax, inspired by Cypher. Vertex elements are denoted by () and edge elements are denoted by []. Here is an example of a pattern-matching query where we find friends of Jan and return their first names.

FROM GRAPH_TABLE(snb
MATCH (a:Person WHERE a.firstName = 'Jan')-[k:Knows]->(b:Person)
COLUMNS (b.firstName)
);

The result will be:

┌───────────┐
│ firstName │
│ varchar │
├───────────┤
│ Ali │
│ Otto │
│ Bryn │
│ Hans │
└───────────┘

Thanks to DuckDB’s friendlier syntax (blog post 1, blog post 2) we can omit the SELECT and instead use the COLUMNS clause.

The previous query featured a right-directed edge, ()-[]->(), meaning that the left node pattern is the source, and the right is the destination.

DuckPGQ also supports the following edge types:

    Left-directed edge:()<-[]-()
        The source of the edge is on the right side, the destination is on the left side.
    Any-directed edge: ()-[]-()
        The relationship can exist in any direction.
    Left-right-directed edge: ()<-[]->()
        The relationship must exist in both directions.

OPTIONAL MATCH is currently not supported but will be in a future update.
Path-finding

Another significant feature of SQL/PGQ is the introduction of a more concise syntax for path-finding within a query. This enables us to find the shortest path length between any pairs of nodes in the graph.

DuckPGQ only supports finding ANY SHORTEST path between nodes, which is non-deterministic.

To query the shortest path length between Jan and the first five persons sorted alphabetically, use the following query:

FROM GRAPH_TABLE (snb
MATCH p = ANY SHORTEST (a:Person WHERE a.firstName = 'Jan')-[k:knows]->+(b:Person)
COLUMNS (path_length(p), b.firstName)
)
ORDER BY firstName
LIMIT 5;

The result will be:

┌────────────────┬─────────────┐
│ path_length(p) │ firstName │
│ int64 │ varchar │
├────────────────┼─────────────┤
│ 3 │ Abdul Haris │
│ 2 │ Aleksandr │
│ 2 │ Alexei │
│ 2 │ Ali │
│ 1 │ Ali │
└────────────────┴─────────────┘

The previous query showed the + syntax, which is syntactic sugar for finding the paths with a lower bound of 1, and no upper bound. This can also be denoted as ()-[]->{1,}() .

Other options for path-finding are:

    Kleene star * : Lower bound of 0, no upper bound.
    {n, m}: Lower bound of n (where n > 0) and upper bound of m (where m ≥ n).
    {,m}: Lower bound of 0, upper bound of m .
    {n,} : Lower bound of n , no upper bound.

Retrieving the path

DuckPGQ also allows you to retrieve the rowid’s of the nodes and edges that are on the shortest path by adding element_id(<path variable>) in the COLUMNS clause.

Other options are:

    vertices(<path variable>) : Returns the rowid ’s of the vertices on the shortest path.
    edges(<path variable>) : Returns the rowid ’s of the edges on the shortest path.
    path_length(<path variable>): Returns the path length of the shortest path.

The following query shows an example:

FROM GRAPH_TABLE (snb
MATCH p = ANY SHORTEST (a:Person WHERE a.firstName = 'Jan')-[k:knows]->+(b:Person)
COLUMNS (element_id(p), vertices(p), edges(p), path_length(p), b.firstName)
)
ORDER BY firstName
LIMIT 5;

The result will be:

┌───────────────────────────┬────────────────┬─────────────┬────────────────┬─────────────┐
│ element_id(p) │ vertices(p) │ edges(p) │ path_length(p) │ firstName │
│ int64[] │ int64[] │ int64[] │ int64 │ varchar │
├───────────────────────────┼────────────────┼─────────────┼────────────────┼─────────────┤
│ [1, 3, 5, 22, 26, 66, 44] │ [1, 5, 26, 44] │ [3, 22, 66] │ 3 │ Abdul Haris │
│ [1, 5, 33, 79, 39] │ [1, 33, 39] │ [5, 79] │ 2 │ Aleksandr │
│ [1, 3, 5, 24, 32] │ [1, 5, 32] │ [3, 24] │ 2 │ Alexei │
│ [1, 3, 5, 21, 21] │ [1, 5, 21] │ [3, 21] │ 2 │ Ali │
│ [1, 3, 5] │ [1, 5] │ [3] │ 1 │ Ali │
└───────────────────────────┴────────────────┴─────────────┴────────────────┴─────────────┘

Graph Functions

DuckPGQ provides a range of graph algorithms that allow you to conveniently analyze your data directly within DuckDB.

Supported algorithms:

    Local Clustering Coefficient
    Weakly Connected Component
    PageRank

On this page, we will use the Person and Person_knows_Person tables from the LDBC Social Network Benchmark (SNB) dataset. These tables represent individuals and their relationships within the network.

-- Create the Person table
CREATE TABLE Person AS
SELECT \* FROM 'https://gist.githubusercontent.com/Dtenwolde/2b02aebbed3c9638a06fda8ee0088a36/raw/8c4dc551f7344b12eaff2d1438c9da08649d00ec/person-sf0.003.csv';

-- Create the Person_knows_person table
CREATE TABLE Person_knows_person AS
SELECT \* FROM 'https://gist.githubusercontent.com/Dtenwolde/81c32c9002d4059c2c3073dbca155275/raw/8b440e810a48dcaa08c07086e493ec0e2ec6b3cb/person_knows_person-sf0.003.csv';

-- Create the property graph
CREATE PROPERTY GRAPH snb
VERTEX TABLES (
Person
)
EDGE TABLES (
Person_knows_person
SOURCE KEY (Person1Id) REFERENCES Person (id)
DESTINATION KEY (Person2Id) REFERENCES Person (id)
LABEL Knows
);

Local Clustering Coefficient

The Local Clustering Coefficient (LCC) measures how closely a node's neighbours are connected, forming a local cluster. In this example, we calculate the LCC for each person in the graph.

The query syntax for calculating the local clustering coefficient is as follows:

SELECT \*
FROM local_clustering_coefficient(<property graph>, <vertex label>, <edge label>);

    <property graph>: The name of the property graph (e.g., snb).
    <vertex label>: The vertex label representing the nodes (e.g., Person).
    <edge label>: The edge label representing the relationship (e.g., Person_knows_person).

The query returns a result with two columns:

    the primary key of the vertex table (in this schema the id of the person)
    local_clustering_coefficient for each node (person), representing how interconnected their neighbours are.

The underlying graph is treated as undirected for the purposes of this calculation.

Example query:

SELECT \*
FROM local_clustering_coefficient(snb, Person, Knows);

Weakly Connected Component

The Weakly Connected Component (WCC) identifies groups of nodes where any two nodes are connected by a path, regardless of edge direction. In this example, we calculate the WCC for each person in the graph.

The query syntax for calculating the weakly connected components is as follows:

SELECT \*
FROM weakly_connected_component(<property graph>, <vertex label>, <edge label>);

    <property graph>: The name of the property graph (e.g., snb).
    <vertex label>: The vertex label representing the nodes (e.g., Person).
    <edge label>: The edge label representing the relationships (e.g., Knows).

The query returns a result with two columns:

    primary key of the vertex table (the id of the person in this schema)
    componentId: the minimum rowid of all nodes in the connected component, where nodes with the same componentId are part of the same weakly connected component.

The underlying graph is treated as undirected during the calculation of weakly connected components.

Example query:

SELECT \*
FROM weakly_connected_component(snb, Person, Knows);

PageRank

The PageRank algorithm ranks nodes based on their importance in a directed graph, where a node's rank is determined by the ranks of the nodes linking to it. In this example, we calculate the PageRank for each person in the graph.

The query syntax for calculating PageRank is as follows:

SELECT \*
FROM pagerank(<property graph>, <vertex label>, <edge label>);

    <property graph>: The name of the property graph (e.g., snb).
    <vertex label>: The vertex label representing the nodes (e.g., Person).
    <edge label>: The edge label representing the relationships (e.g., Knows).

The query returns a result with two columns:

    primary key of the vertex table (the id of the person in this schema)
    PageRank for each node, representing its importance in the graph.

This calculation assumes a directed edge table. The algorithm uses a damping factor of 0.85 and a tolerance of 1e-6.

Example query:

SELECT \*
FROM pagerank(snb, Person, Knows);

Property Graph
CREATE

The first step in using SQL/PGQ is creating a property graph as a layer on top of your data. In DuckPGQ, property graphs are transient; they only exist as long as the connection to the database is open.

As of community version v0.1.0 released with DuckDB v1.1.3 property graphs are persistent and are synchronised between connections.

The tables will be divided into vertex tables and edge tables, having a primary key-foreign key relationship. An edge table should have a column defining the source node and a column describing the destination node.

To create a property graph the syntax is as follows:

CREATE [ OR REPLACE ] PROPERTY GRAPH [ IF NOT EXISTS ] (<property graph name>
VERTEX TABLES (
<vertex table>
[, <vertex table> ]
)
[ EDGE TABLES (
<edge table>
[, <edge table ] ) ];

At least one <vertex table> must be specified to create a valid property graph. The EDGE TABLES are optional. For example to make a property graph over a subset of the Social Network Benchmark dataset from LDBC:

CREATE PROPERTY GRAPH snb
VERTEX TABLES (
Person,
Message,
Forum
)
EDGE TABLES (
Person_knows_person SOURCE KEY (Person1Id) REFERENCES Person (id)
DESTINATION KEY (Person2Id) REFERENCES Person (id)
LABEL Knows,
Forum_hasMember_Person SOURCE KEY (ForumId) REFERENCES Forum (id)
DESTINATION KEY (PersonId) REFERENCES Person (id)
LABEL hasMember,
Person_likes_Message SOURCE KEY (PersonId) REFERENCES Person (id)
DESTINATION KEY (id) REFERENCES Message (id)
LABEL likes_Message
);

Vertex table

<table name> [ AS <table name alias> ] [ PROPERTIES (<Properties>) ] [ LABEL <Label> ]

Only the table name is required for the vertex table; the table name alias, properties, and label are optional.
Edge table

To define the edge table, it is necessary to specify the table name, along with the source and destination keys.

In the following example, the source of the edge references the Person table, where the primary key is id and the foreign key is personId. The destination references the Message table, where both the primary key and the foreign key are id.

Person_likes_Message SOURCE KEY (PersonId) REFERENCES Person (id)
DESTINATION KEY (id) REFERENCES Message (id)
LABEL likes_Message

The LABEL and the PROPERTIES are optional.
Pre-defined PK-FK relations

If the PK-FK relationships have already been defined during table creation, it is not necessary to repeat them when creating a property graph, unless this leads to ambiguity. The system will automatically infer the relationships based on the existing PK-FK constraints.

Simple Example

Given the following schema:

CREATE TABLE a (
id BIGINT PRIMARY KEY,
name VARCHAR
);
CREATE TABLE b (
id BIGINT PRIMARY KEY,
description VARCHAR
);
CREATE TABLE edge_ab (
id BIGINT PRIMARY KEY,
src BIGINT REFERENCES a(id),
dst BIGINT REFERENCES b(id)
);

The following is sufficient during property graph creation:

CREATE PROPERTY GRAPH g_relationship
VERTEX TABLES (a, b)
EDGE TABLES (edge_ab SOURCE a DESTINATION b);

Here, the system can infer that the column src in edge_ab references the primary key in a, and dst references the primary key in b.

Handling Ambiguity in PK-FK Relationships

If an edge table has more than one PK-FK relationship defined with the same vertex table, it becomes ambiguous which relationship to use for the SOURCE and DESTINATION. In this case, you must explicitly define both the source and destination keys.

Consider the following schema:

CREATE TABLE Person(
id BIGINT PRIMARY KEY
);

CREATE TABLE Person_knows_Person(
Person1Id BIGINT REFERENCES Person (id),
Person2Id BIGINT REFERENCES Person (id)
);

Attempting to create the property graph without explicitly defining the primary and foreign keys will result in an error:

CREATE PROPERTY GRAPH (snb
VERTEX TABLES (Person)
EDGE TABLES (Person_knows_Person SOURCE Person DESTINATION Person);

Error:

Invalid Error: Multiple primary key - foreign key relationships detected between Person_knows_Person and Person. Please explicitly define the primary key and foreign key columns using `SOURCE KEY <primary key> REFERENCES Person <foreign key>`

Resolving Ambiguity

To resolve this, you must explicitly define the primary and foreign key columns for the source and destination relationships, as follows:

CREATE PROPERTY GRAPH snb
VERTEX TABLES (Person)
EDGE TABLES (Person_knows_Person
SOURCE KEY (Person1Id) REFERENCES Person(id)
DESTINATION KEY (Person2Id) REFERENCES Person(id));

By specifying the KEY and REFERENCES clauses explicitly, you remove any ambiguity, allowing the graph creation to proceed successfully.
Inheritance

Inheritance in relational databases can be achieved by using a special column that indicates the type of entity, allowing a single table to store multiple types of related entities. This approach is often referred to as single-table inheritance.

Consider a table called Organisation that can represent different types of organizations, such as companies and universities. We use a special column called typemask to indicate the type of organization.
id type name typeMask
6466 University National_Chung_Hsing_University 2
812 Company Tepavia_Trans 1
7677 University University_of_Arkansas_Graduate_School 2
5103 University Villahermosa_Institute_of_Technology 2
231 Company Kivalliq_Air 1

    Table Name: Organisation
    Special Column: typemask - This column indicates the type of organization. It can take values such as company and university.
    Primary Key: OrganisationID - This uniquely identifies each organization in the table.

In this example, the Organisation table can store different types of organizations by using the typemask column to distinguish between them. This approach allows for flexibility and avoids the need for multiple tables to represent each type of organization.
Inheritance Definition

The inheritance is defined using the typemask column:

Organisation LABEL Organisation IN typemask(company, university)

Here, LABEL Organisation indicates that the table Organisation is being defined. The IN typemask(company, university) part specifies that the typemask column will be used to indicate whether a record is a company or a university.

By using this approach, you can efficiently manage different types of related entities within a single table, simplifying your database design and queries.

Within a MATCH statement, we can now use the labels company or university to create a filter on these types:

FROM GRAPH_TABLE (snb
MATCH (a:person)-[w:worksAt]->(c:company)
COLUMNS (a.firstName, c.name)
)

Properties

Properties can restrict the columns used in a SQL/PGQ query.

The specifications allow several options:

    PROPERTIES (column [, <column>]): List the columns allowed from the original table
    PROPERTIES [ARE] ALL COLUMNS [EXCEPT (column [, column])]: Allow all columns from the original table except the columns listed in the EXCEPT list.
    NO PROPERTIES: Allow no columns from the original table

Label

The label can be used to reference the vertex or edge table in future PGQ queries. However, it is completely optional and when omitted the original table name can be used in PGQ queries. It can be useful to make abbreviations of table names. In the following example, no label is specified for Person, but for Person_knows_Person we create the label Knows.

CREATE PROPERTY GRAPH snb
VERTEX TABLES (
Person
)
EDGE TABLES (
Person_knows_person SOURCE KEY (Person1Id) REFERENCES Person (id)
DESTINATION KEY (Person2Id) REFERENCES Person (id)
LABEL Knows
);

FROM GRAPH_TABLE (snb
MATCH (p:Person)-[k:Knows]->(p2:Person)
COLUMNS (p.id, p2.id)
)
LIMIT 1;

┌───────┬────────────────┐
│ id │ id_1 │
│ int64 │ int64 │
├───────┼────────────────┤
│ 14 │ 10995116277782 │
└───────┴────────────────┘

DESCRIBE

Once you have created a property graph, you can use DESCRIBE PROPERTY GRAPH to show information about it, such as the table name, label, and in the case of edge tables their source and destination keys. For the property graph snb created above, the output will be:

┌─────────────────────┬─────────┬─────────────────┬──────────────┬───────────┬─────────────┬───────────────────┬────────────────┬────────────────┬───────────────┬────────────┐
│ table_name │ label │ is_vertex_table │ source_table │ source_pk │ source_fk │ destination_table │ destination_pk │ destination_fk │ discriminator │ sub_labels │
│ varchar │ varchar │ boolean │ varchar │ varchar[] │ varchar[] │ varchar │ varchar[] │ varchar[] │ varchar │ varchar[] │
├─────────────────────┼─────────┼─────────────────┼──────────────┼───────────┼─────────────┼───────────────────┼────────────────┼────────────────┼───────────────┼────────────┤
│ Person │ person │ true │ │ │ │ │ │ │ │ │
│ Person_knows_person │ knows │ false │ Person │ [id] │ [Person1Id] │ Person │ [id] │ [Person2Id] │ │ │
└─────────────────────┴─────────┴─────────────────┴──────────────┴───────────┴─────────────┴───────────────────┴────────────────┴────────────────┴───────────────┴────────────┘

DROP

Delete a property graph with the name pg

DROP PROPERTY GRAPH pg

Delete a property graph with the name pg; do not throw an error if the property graph does not exist:

DROP IF EXISTS PROPERTY GRAPH pg

Adding IF EXISTS will not throw an error if <property graph name> does not exist. Omitting this will result in a BinderException if the <property graph name> does not exist.
ALTER

To be supported in a future version For now, dropping and recreating the property graph is required if you wish to alter the property graph.

Pragmas
show_property_graphs

The show_property_graphs pragma lists all currently registered property graphs in the DuckPGQ extension.

Property graphs are registered via the CREATE PROPERTY GRAPH statement, and this pragma allows you to inspect which graphs are available in the current database session.
Syntax

PRAGMA show_property_graphs;
Example

CREATE PROPERTY GRAPH my_graph (
VERTEX TABLE people KEY (id),
EDGE TABLE knows SOURCE people KEY (source_id) DESTINATION people KEY (target_id)
);

PRAGMA show_property_graphs;

Output:

## property_graph

my_graph

Notes

    This pragma internally runs: SELECT DISTINCT property_graph FROM __duckpgq_internal;

create_vertex_table

The create_vertex_table pragma in DuckPGQ generates a vertex table based on the distinct vertex identifiers found in the source and destination columns of an edge table. This is useful when your graph model is defined by an edge table, and you want to derive a vertex table automatically.
Syntax

PRAGMA create_vertex_table(‘edge_table’, ‘source_column’, ‘destination_column’, ‘vertex_table_name’, ‘id_column_name’);
Parameters
Parameter Description
edge_table Name of the edge table
source_column Column in the edge table representing the source vertex
destination_column Column in the edge table representing the destination vertex
vertex_table_name Name of the new vertex table to be created
id_column_name Name of the column to be used for vertex IDs in the new table
Example

Given an edge table like:

CREATE TABLE edges (src INT, dst INT);

You can create a vertex table from it:

PRAGMA create_vertex_table('edges', 'src', 'dst', 'vertices', 'id');

This will generate and execute the following SQL under the hood:

CREATE TABLE vertices AS
SELECT DISTINCT id FROM (
SELECT src AS id FROM edges
UNION ALL
SELECT dst AS id FROM edges
);

Notes

    Both source and destination values are combined and deduplicated using DISTINCT.
    The resulting vertex_table_name will have a single column named id_column_name, containing all unique vertex IDs.
    This pragma is a utility to simplify building a property graph schema from existing relational data.
