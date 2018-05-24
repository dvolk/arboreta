import yaml

cfg = None

with open("arboreta.yaml", "r") as f:
    cfg = yaml.load(f)
