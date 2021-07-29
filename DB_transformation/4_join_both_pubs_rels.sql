--Find merged OpenAIRE and CORD19 publications that are linked to OpenAIRE projects through relationships
select * into join_all_pubs_rels_src
from join_oa_cord_doi join oa_relationships_final
on join_oa_cord_doi.oaid = oa_relationships_final.src_id
