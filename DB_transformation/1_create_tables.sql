GO
create table history
(blobname varchar(255),
PRIMARY key (blobname));

GO
create table oa_publications_final
(oaid varchar(255),
title varchar(1024),
doi varchar(255),
pmid varchar(255),
arxiv varchar(255),
publication_date varchar(255)
);

GO
create table oa_projects_final
(oaid varchar(255),
code varchar(255),
title varchar(1024),
startdate varchar(255),
enddate varchar(255),
currency varchar(122),
amount float,
jurisdiction varchar(255),
longname varchar(255),
shortname varchar(255)
)

GO
create table oa_relationships_final
(src_id varchar(255),
src_type varchar(255),
trg_id varchar(255),
trg_type varchar(255),
rel_type varchar(255)
);

--join cord_publications and oa_publications
--select top(100) *
--from oa_publications join cord_publications
--on oa_publications.doi = cord_publications.doi
--select oaid from oa_publications
--where doi = '10.1016/0304-4165(82)90267-7'
-- Create a nonclustered index on a table or view
--CREATE INDEX i1 ON oa_publications(oaid)
/*select *
from INFORMATION_SCHEMA.COLUMNS
where TABLE_NAME='oa_publications'*/
/*ALTER TABLE oa_publications 
alter COLUMN doi varchar(256)*/
/*UPDATE cord_publications SET doi = LEFT(doi, 255)*/