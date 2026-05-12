from dagfactory import load_yaml_dags
import os

# Path to your YAML file
dags_path = os.path.dirname(__file__)
print(f"{dags_path=}")

# To load all yaml dags, uncomment below:
load_yaml_dags(globals_dict=globals(), dags_folder=dags_path,)

yaml_path = os.path.join(os.path.dirname(__file__), "airflow_capability_demo_yaml.yaml")
print(f"{yaml_path=}")

## This single function now replaces the manual class instantiation
print(f"Attempting to load yaml based dags from {yaml_path}")
load_yaml_dags(globals_dict=globals(), config_filepath=yaml_path)
print("DONE")

