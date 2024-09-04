from pathlib import Path
from dagster_dbt import DbtProject

parent = Path(__file__).parents[1]
proj_path = parent.joinpath("analytics").resolve()

dbt_project = DbtProject(project_dir=proj_path)
dbt_project.prepare_if_dev()
