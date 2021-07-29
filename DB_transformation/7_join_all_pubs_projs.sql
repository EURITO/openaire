--Before running this, run Step 6 in Jupyter Notebook to obtain all_pubs_linked table.
--Join all linked publications to projects
select * into join_all_pubs_projs
from all_pubs_linked join oa_projects_final
on all_pubs_linked.project_id = oa_projects_final.oaid