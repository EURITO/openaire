--Join OpenAIRE publications and CORD19 publications over 'doi' field
select * into join_oa_cord_doi
from oa_publications_final join cord_publications_final
on oa_publications_final.oa_doi = cord_publications_final.cord_doi
where oa_publications_final.oa_doi <> 'empty'