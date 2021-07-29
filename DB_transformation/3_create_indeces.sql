-- Create a nonclustered index on tables
CREATE INDEX oa_3 ON oa_publications_final(oaid, doi, pmid, arxiv)
CREATE INDEX oa_rels_src ON oa_relationships_final(src_type)